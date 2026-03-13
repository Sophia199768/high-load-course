package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.resume

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val registry : MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val counter = Counter.builder("queries.amount").tag("name", "ordersAfter").register(registry)
    private val summary =  DistributionSummary
        .builder("request_latency_seconds")
        .tags("service", "payment")
        .publishPercentiles(0.5, 0.95, 0.99)
        .register(registry)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val retryCounter = Counter
        .builder("payment_retry_count")
        .tag("account", accountName)
        .register(registry)

    private val callNotPermittedCounter = Counter
        .builder("payment_circuit_breaker_rejected_total")
        .tag("account", accountName)
        .register(registry)

    private val currentInflight = AtomicInteger(0)

    private val inflightGauge = Gauge
        .builder("inflight_requests", this) {
            it.currentInflight.get().toDouble()
        }.tag("account", properties.accountName).register(registry)

    private val parallelRequests = properties.parallelRequests
    private val dispatcherMaxRequests = parallelRequests
    private val dispatcherMaxRequestsPerHost = parallelRequests
    private val connectionPoolMaxIdleConnections = parallelRequests
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val windowRetryBackoffMs = 1L
    private val virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor()
    private val paymentScope = CoroutineScope(virtualThreadExecutor.asCoroutineDispatcher() + SupervisorJob())
    private val minimumDeadlineBudgetMs = 30L
    private val readTimeout = Duration.ofSeconds(10)
    private val minAdaptiveTimeoutMs = 300L
    private val maxAdaptiveTimeoutMs = 2_000L
    private val hedgeDelayMs = 500L

    private val client = HttpClient.newBuilder()
        .executor(virtualThreadExecutor)
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(5))
        .build()
    private val semaphoreToLimitParallelRequest = NonBlockingOngoingWindow(parallelRequests)
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val circuitBreaker = CircuitBreaker.of(
        "payment:$accountName",
        CircuitBreakerConfig.custom()
            .slidingWindowType(SlidingWindowType.TIME_BASED)
            .slidingWindowSize(10)
            .minimumNumberOfCalls(20)
            .failureRateThreshold(70f)
            .slowCallRateThreshold(80f)
            .slowCallDurationThreshold(Duration.ofMillis(800))
            .waitDurationInOpenState(Duration.ofSeconds(1))
            .permittedNumberOfCallsInHalfOpenState(5)
            .automaticTransitionFromOpenToHalfOpenEnabled(true)
            .recordException { it is ExternalServiceException }
            .build()
    )
    @Volatile
    private var emaLatency = properties.averageProcessingTime.toMillis().coerceAtLeast(minAdaptiveTimeoutMs)

    private fun updateLatency(duration: Long) {
        val alpha = 0.2
        emaLatency = (alpha * duration + (1 - alpha) * emaLatency).toLong()
    }

    private val maxAttemptAmount = 6
    private val minTimeToMakeRequest = 60

    private fun requestRetryDelayMs(attempt: Int): Long {
        val expBackoff = (1L shl (attempt - 1).coerceAtMost(7)) * 5L
        val jitter = ThreadLocalRandom.current().nextLong(0, 15)
        return (expBackoff + jitter).coerceAtMost(500L)
    }

    init {
        circuitBreaker.eventPublisher
            .onStateTransition { event ->
                logger.info("[$accountName] CircuitBreaker transition: ${event.stateTransition}")
            }
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Void> {
        val resultFuture = CompletableFuture<Void>()

        paymentScope.launch {
            while (semaphoreToLimitParallelRequest.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
                val timeUntilDeadline = deadline - now()
                if (timeUntilDeadline <= minimumDeadlineBudgetMs) {
                    logger.warn("[$accountName] Payment $paymentId rejected - deadline too close (${timeUntilDeadline}ms, need >= ${minimumDeadlineBudgetMs}ms)")
                    resultFuture.complete(null)
                    return@launch
                }
                delay(windowRetryBackoffMs)
            }

            if (deadline - now() <= minimumDeadlineBudgetMs) {
                semaphoreToLimitParallelRequest.releaseWindow()
                resultFuture.complete(null)
                return@launch
            }

            currentInflight.incrementAndGet()
            val transactionId = UUID.randomUUID()

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            try {
                performRequestWithHedgingAsync(paymentId, amount, transactionId, deadline)
            } finally {
                currentInflight.decrementAndGet()
                semaphoreToLimitParallelRequest.releaseWindow()
            }

            resultFuture.complete(null)
        }
        return resultFuture
    }

    private suspend fun <T> CompletableFuture<T>.awaitSuspending(): T =
        suspendCancellableCoroutine { cont ->
            whenComplete { value, throwable ->
                if (throwable != null) {
                    cont.resumeWith(Result.failure(throwable))
                } else {
                    cont.resume(value)
                }
            }
            cont.invokeOnCancellation { cancel(true) }
        }


    private suspend fun performRequestWithHedgingAsync(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ) {
        for (attempt in 1..maxAttemptAmount) {
            if (deadline - now() <= minimumDeadlineBudgetMs) return

            while (!slidingWindowRateLimiter.tick()) {
                if (deadline - now() <= minimumDeadlineBudgetMs) return
                slidingWindowRateLimiter.tickAsync().get()
                if (deadline - now() <= minimumDeadlineBudgetMs) return
            }

            val remaining = deadline - now()
            if (remaining <= minimumDeadlineBudgetMs) return

            val resultFuture = CompletableFuture<Pair<Boolean, String?>>()
            buildAndEnqueue(paymentId, amount, transactionId, deadline, resultFuture)

            val hedgeTask = paymentScope.launch {
                delay(hedgeDelayMs)
                if (!resultFuture.isDone) {
                    buildAndEnqueue(paymentId, amount, transactionId, deadline, resultFuture)
                }
            }

            val timeoutMs = (deadline - now() - minimumDeadlineBudgetMs).coerceAtLeast(1L)
            val (success, message) = try {
                withTimeout(timeoutMs) { resultFuture.awaitSuspending() }
            } catch (e: Exception) {
                Pair(false, e.message ?: "timeout")
            }
            hedgeTask.cancel()

            if (success) {
                paymentESService.update(paymentId) {
                    it.logProcessing(true, now(), transactionId, reason = message)
                }
                return
            }

            if (attempt >= maxAttemptAmount) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = message)
                }
                return
            }

            if (message == "rejected by circuit breaker") {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = message)
                }
                return
            }

            retryCounter.increment()
            delay(requestRetryDelayMs(attempt))
        }
    }

    private fun buildAndEnqueue(paymentId: UUID, amount: Int, transactionId: UUID, deadline: Long, resultFuture: CompletableFuture<Pair<Boolean, String?>>) {
        if (!circuitBreaker.tryAcquirePermission()) {
            val state = circuitBreaker.state
            if (state == CircuitBreaker.State.OPEN) {
                logger.debug("[$accountName] CircuitBreaker OPEN, payment $paymentId rejected")
            }
            resultFuture.complete(Pair(false, "rejected by circuit breaker"))
            return
        }

        val timeoutByLatency = emaLatency.coerceAtLeast(minAdaptiveTimeoutMs).coerceAtMost(maxAdaptiveTimeoutMs)
        val timeoutByDeadline = (deadline - now() - minimumDeadlineBudgetMs).coerceAtLeast(minAdaptiveTimeoutMs)
        val timeout = minOf(timeoutByLatency, timeoutByDeadline)

        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://$paymentProviderHostPort/external/process" +
                    "?serviceName=${properties.serviceName}" +
                    "&token=$token" +
                    "&accountName=${properties.accountName}" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount"))
            .header("x-idempotency-key", transactionId.toString())
            .timeout(Duration.ofMillis(timeout))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val requestStart = now()
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .whenComplete { response, throwable ->
                val responseAt = now()
                val callDuration = responseAt - requestStart
                updateLatency(callDuration)

                if (throwable != null) {
                    circuitBreaker.onError(callDuration, TimeUnit.MILLISECONDS, ExternalServiceException("I/O error", throwable))
                    resultFuture.complete(Pair(false, throwable.message ?: "error"))
                    return@whenComplete
                }

                if (response == null) {
                    circuitBreaker.onError(callDuration, TimeUnit.MILLISECONDS, ExternalServiceException("empty response"))
                    resultFuture.complete(Pair(false, "error"))
                    return@whenComplete
                }

                val rawBody = response.body()
                if (response.statusCode() !in 200..299) {
                    circuitBreaker.onError(
                        callDuration,
                        TimeUnit.MILLISECONDS,
                        ExternalServiceException("HTTP ${response.statusCode()}")
                    )
                    resultFuture.complete(Pair(false, "http ${response.statusCode()}"))
                    return@whenComplete
                }

                if (rawBody.isBlank()) {
                    circuitBreaker.onError(
                        callDuration,
                        TimeUnit.MILLISECONDS,
                        ExternalServiceException("empty response body")
                    )
                    resultFuture.complete(Pair(false, "empty body"))
                    return@whenComplete
                }

                val body = try {
                    mapper.readValue(rawBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    circuitBreaker.onError(
                        callDuration,
                        TimeUnit.MILLISECONDS,
                        ExternalServiceException("invalid response body", e)
                    )
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: $rawBody")
                    resultFuture.complete(Pair(false, e.message ?: "invalid response body"))
                    return@whenComplete
                }

                summary.record(callDuration / 1000.0)
                counter.increment()
                circuitBreaker.onSuccess(callDuration, TimeUnit.MILLISECONDS)

                if (body.result) {
                    resultFuture.complete(Pair(true, body.message))
                } else {
                    resultFuture.complete(Pair(false, body.message))
                }
            }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun isAvailable(): Boolean = circuitBreaker.state != CircuitBreaker.State.OPEN
}

public fun now() = System.currentTimeMillis()

private class ExternalServiceException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
