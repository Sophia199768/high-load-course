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
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
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
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val windowRetryBackoffMs = 1L
    private val rateLimiterRetryBackoffMs = 0L
    private val requestRetryBackoffMs = 0L

    private val virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor()
    private val minimumDeadlineBudgetMs = 100L
    private val maxWindowAcquireWaitMs = 3_000L

    private val client = OkHttpClient.Builder()
        .connectionPool(ConnectionPool(
            maxIdleConnections = parallelRequests,
            keepAliveDuration = 5,
            timeUnit = TimeUnit.MINUTES
        ))
        .dispatcher(Dispatcher(virtualThreadExecutor).apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .connectTimeout(Duration.ofSeconds(5))
        .build()
    private val semaphoreToLimitParallelRequest = NonBlockingOngoingWindow(parallelRequests)
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    @Volatile
    private var emaLatency = properties.averageProcessingTime.toMillis().coerceAtLeast(500L)

    private fun updateLatency(duration: Long) {
        val alpha = 0.2
        emaLatency = (alpha * duration + (1 - alpha) * emaLatency).toLong()
    }

    private val maxAttemptAmount = 3
    private val minTimeToMakeRequest = 60
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
                CompletableFuture.delayedExecutor(windowRetryBackoffMs, TimeUnit.MILLISECONDS).execute {
                    tryAcquireSlotAndSubmit(paymentId, amount, paymentStartedAt, deadline, resultFuture) }
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


                performRequestWithRetryAsync(paymentId, amount, transactionId, deadline, 1)
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

    private fun performRequestWithRetryAsync(
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
            CompletableFuture.delayedExecutor(rateLimiterRetryBackoffMs, TimeUnit.MILLISECONDS)
                .execute {
                    performRequestWithRetryAsync(paymentId, amount, transactionId, deadline, attempt)
                        .whenComplete { _, _ -> future.complete(null) }
                }
            return future
        }

        val timeout = emaLatency.coerceAtLeast(800).coerceAtMost(10_000)

        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process" +
                    "?serviceName=${properties.serviceName}" +
                    "&token=$token" +
                    "&accountName=${properties.accountName}" +
                    "&transactionId=$transactionId" +
                    "&paymentId=$paymentId" +
                    "&amount=$amount")
            .post(emptyBody)
            .build()

        val call = client.newCall(request)
        call.timeout().timeout(timeout, TimeUnit.MILLISECONDS)

        val requestStart = now()

        call.enqueue(object : Callback {

            override fun onFailure(call: Call, e: IOException) {
                updateLatency(now() - requestStart)

                if (attempt < maxAttemptAmount && deadline - now() > minTimeToMakeRequest) {
                    retryCounter.increment()
                    CompletableFuture.delayedExecutor(requestRetryBackoffMs, TimeUnit.MILLISECONDS)
                        .execute {
                            performRequestWithRetryAsync(paymentId, amount, transactionId, deadline, attempt + 1)
                                .whenComplete { _, _ -> future.complete(null) }
                        }
                } else {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message ?: "error")
                    }
                    future.complete(null)
                }
            }

            override fun onResponse(call: Call, response: Response) {
                response.use {
                    updateLatency(now() - requestStart)

                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    summary.record((now() - requestStart) / 1000.0)
                    counter.increment()

                    if (body.result || attempt >= maxAttemptAmount) {
                        // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                        // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        future.complete(null)
                    } else {
                        retryCounter.increment()
                        CompletableFuture.delayedExecutor(requestRetryBackoffMs, TimeUnit.MILLISECONDS)
                            .execute {
                                performRequestWithRetryAsync(paymentId, amount, transactionId, deadline, attempt + 1)
                                    .whenComplete { _, _ -> future.complete(null) }
                            }
                    }
                }
            }
        })

        return future
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()