package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.*
import okhttp3.Protocol
import java.io.IOException
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

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

        val emptyBody = RequestBody.create(null, ByteArray(0))
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
    private val retryScheduler: ScheduledExecutorService =
        Executors.newScheduledThreadPool(
            1,
            Thread.ofVirtual().factory()
        )

    private val virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor()
    private val minimumDeadlineBudgetMs = 30L
    private val readTimeout = Duration.ofSeconds(10)
    private val minAdaptiveTimeoutMs = 10L
    private val maxAdaptiveTimeoutMs = 2_000L
    private val hedgeDelayMs: Long
        get() = (emaLatency * 1.5).toLong().coerceIn(50L, 800L)

    private val client = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(
            maxIdleConnections = connectionPoolMaxIdleConnections,
            keepAliveDuration = 5,
            timeUnit = TimeUnit.MINUTES
        ))
        .dispatcher(Dispatcher(virtualThreadExecutor).apply {
            maxRequests = dispatcherMaxRequests
            maxRequestsPerHost = dispatcherMaxRequestsPerHost
        })
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .connectTimeout(Duration.ofSeconds(5))
        .readTimeout(readTimeout)
        .build()
    private val semaphoreToLimitParallelRequest = NonBlockingOngoingWindow(parallelRequests)
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
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
    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Void> {
        val resultFuture = CompletableFuture<Void>()
        tryAcquireSlotAndSubmit(paymentId, amount, paymentStartedAt, deadline, resultFuture)
        return resultFuture
    }

    private fun tryAcquireSlotAndSubmit(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        resultFuture: CompletableFuture<Void>
    ) {
        val timeUntilDeadline = deadline - now()
        if (timeUntilDeadline <= minimumDeadlineBudgetMs) {
            logger.warn("[$accountName] Payment $paymentId rejected - deadline too close (${timeUntilDeadline}ms, need >= ${minimumDeadlineBudgetMs}ms)")
            resultFuture.complete(null)
            return
        }

        when (semaphoreToLimitParallelRequest.putIntoWindow()) {
            is NonBlockingOngoingWindow.WindowResponse.Fail -> {
                if (deadline - now() <= minimumDeadlineBudgetMs) {
                    resultFuture.complete(null)
                    return
                }
                retryScheduler.schedule(
                    { tryAcquireSlotAndSubmit(paymentId, amount, paymentStartedAt, deadline, resultFuture) },
                    windowRetryBackoffMs,
                    TimeUnit.MILLISECONDS
                )
            }

            is NonBlockingOngoingWindow.WindowResponse.Success -> {
                currentInflight.incrementAndGet()
                val transactionId = UUID.randomUUID()

                // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
                // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
                paymentESService.update(paymentId) {
                    it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }

                logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")


                performRequestWithHedgingAsync(paymentId, amount, transactionId, deadline, 1)
                    .whenComplete { _, throwable ->
                        currentInflight.decrementAndGet()
                        semaphoreToLimitParallelRequest.releaseWindow()

                        if (throwable != null)
                            resultFuture.completeExceptionally(throwable)
                        else
                            resultFuture.complete(null)
                    }
            }
        }
    }

    private fun performRequestWithHedgingAsync(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
        attempt: Int
    ): CompletableFuture<Void> {
        val future = CompletableFuture<Void>()

        val remaining = deadline - now()
        if (remaining <= minimumDeadlineBudgetMs || attempt > maxAttemptAmount) {
            future.complete(null)
            return future
        }

        if (!slidingWindowRateLimiter.tick()) {
            slidingWindowRateLimiter.tickAsync()
                .whenComplete { _, _ ->
                    if (deadline - now() <= minimumDeadlineBudgetMs) {
                        future.complete(null)
                    } else {
                        performRequestWithHedgingAsync(paymentId, amount, transactionId, deadline, attempt)
                            .whenComplete { _, _ -> future.complete(null) }
                    }
                }
            return future
        }

        val done = AtomicBoolean(false)
        buildAndEnqueue(paymentId, amount, transactionId, deadline, attempt, future, done)
        val hedgeTask = retryScheduler.schedule({
            if (done.get()) return@schedule
            if (deadline - now() <= minimumDeadlineBudgetMs) return@schedule

            logger.debug("[$accountName] Hedge sent for $paymentId, txId: $transactionId")

            buildAndEnqueue(paymentId, amount, transactionId, deadline, attempt, future, done)
        }, hedgeDelayMs, TimeUnit.MILLISECONDS)

        future.whenComplete { _, _ -> hedgeTask.cancel(false) }

        return future
    }

    private fun buildAndEnqueue(paymentId: UUID, amount: Int, transactionId: UUID, deadline: Long, attempt: Int, future: CompletableFuture<Void>, done: AtomicBoolean): Call {
        val timeoutByLatency = emaLatency.coerceAtLeast(minAdaptiveTimeoutMs).coerceAtMost(maxAdaptiveTimeoutMs)
        val timeoutByDeadline = (deadline - now() - minimumDeadlineBudgetMs).coerceAtLeast(minAdaptiveTimeoutMs)
        val timeout = minOf(timeoutByLatency, timeoutByDeadline)

        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process" +
                    "?serviceName=${properties.serviceName}" +
                    "&token=$token" +
                    "&accountName=${properties.accountName}" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount")
            .header("x-idempotency-key", transactionId.toString())
            .post(emptyBody)
            .build()

        val call = client.newCall(request)
        call.timeout().timeout(timeout, TimeUnit.MILLISECONDS)

        val requestStart = now()

        call.enqueue(object : Callback {

            override fun onFailure(call: Call, e: IOException) {
                val failureAt = now()
                updateLatency(failureAt - requestStart)

                if (done.get()) return
                if (attempt < maxAttemptAmount && deadline - failureAt > minTimeToMakeRequest) {
                    if (done.compareAndSet(false, true)) {
                        retryCounter.increment()
                        retryScheduler.schedule({
                            performRequestWithHedgingAsync(paymentId, amount, transactionId, deadline, attempt + 1)
                                .whenComplete { _, _ -> future.complete(null) }
                        },
                            requestRetryDelayMs(attempt),
                            TimeUnit.MILLISECONDS
                        )
                    }
                } else {
                    if (done.compareAndSet(false, true)) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, failureAt, transactionId, reason = e.message ?: "error")
                        }
                        future.complete(null)
                    }
                }
            }

            override fun onResponse(call: Call, response: Response) {
                response.use {
                    val responseAt = now()
                    updateLatency(responseAt - requestStart)
                    val rawBody = response.body?.string()

                    val body = if (rawBody.isNullOrBlank()) {
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "empty body")
                    } else {
                        try {
                            mapper.readValue(rawBody, ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $rawBody")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                    }

                    summary.record((responseAt - requestStart) / 1000.0)
                    counter.increment()

                    if (body.result) {
                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        if (done.compareAndSet(false, true)) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(true, responseAt, transactionId, reason = body.message)
                            }
                            future.complete(null)
                        }
                    } else {
                        if (attempt >= maxAttemptAmount) {
                            if (done.compareAndSet(false, true)) {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, responseAt, transactionId, reason = body.message)
                                }
                                future.complete(null)
                            }
                        } else {
                            if (done.compareAndSet(false, true)) {
                                retryCounter.increment()
                                retryScheduler.schedule({
                                    performRequestWithHedgingAsync(
                                        paymentId, amount, transactionId, deadline, attempt + 1
                                    ).whenComplete { _, _ -> future.complete(null) }
                                }, requestRetryDelayMs(attempt), TimeUnit.MILLISECONDS)
                            }
                        }
                    }
                }
            }
        })

        return call
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
