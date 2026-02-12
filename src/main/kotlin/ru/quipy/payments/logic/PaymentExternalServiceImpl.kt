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
import ru.quipy.common.utils.OngoingWindow
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

    private val inflightGauge = Gauge
        .builder("inflight_requests", this) { adapter ->
            adapter.currentInflight.get().toDouble()
        }
        .tag("account", accountName)
        .register(registry)

    private val currentInflight = AtomicInteger(0)

    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor()

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
        .writeTimeout(Duration.ofSeconds(10))
        .build()
    private val semaphoreToLimitParallelRequest = OngoingWindow(parallelRequests)
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val lastDurations = LinkedBlockingDeque<Long>(1000)

    /**
     * Минимальное время, которое должно оставаться до дедлайна, чтобы имело смысл
     * отправлять запрос во внешнюю систему.
     *
     * Для "медленных" аккаунтов (большой averageProcessingTime) оставляем верхнюю
     * границу в 5 секунд (как было изначально), чтобы не сломать уже рабочие кейсы.
     * Для очень быстрых аккаунтов (как acc-13 с ~10ms) снижаем порог до сотен миллисекунд,
     * чтобы не отбрасывать платежи преждевременно.
     */
    private fun minTimeRequiredBeforeDeadline(): Long {
        val avgMs = requestAverageProcessingTime.toMillis().coerceAtLeast(1L)
        val base = avgMs * 2 // небольшой запас относительно среднего времени обработки

        return base
            .coerceAtLeast(50L)    // для быстрых аккаунтов не отсекаем почти все запросы
            .coerceAtMost(5_000L)  // не больше 5 секунд, как было в исходной реализации
    }

    private fun quantile(q: Double): Long {
        val copy = lastDurations.toList()
        if (copy.isEmpty()) {
            val avgMs = requestAverageProcessingTime.toMillis().coerceAtLeast(1L)
            return (avgMs * 3).coerceAtLeast(50L)
        }
        val sorted = copy.sorted()
        val indexes = ((sorted.size - 1) * q).toInt().coerceIn(0, sorted.size - 1)
        return sorted[indexes]
    }

    private val maxAttemptAmount = 3
    private val minTimeToMakeRequest = 60
    private fun releaseInflightPermit() {
        currentInflight.decrementAndGet()
        semaphoreToLimitParallelRequest.release()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long): CompletableFuture<Void> {
        val resultFuture = CompletableFuture<Void>()
        var permitAcquired = false

        try {
            val timeUntilDeadline = deadline - now()
            val minTimeRequired = minTimeRequiredBeforeDeadline()
            if (timeUntilDeadline < minTimeRequired) {
                logger.warn("[$accountName] Payment $paymentId rejected - deadline too close (${timeUntilDeadline}ms, need >= ${minTimeRequired}ms)")
                resultFuture.complete(null)
                return resultFuture
            }

            semaphoreToLimitParallelRequest.acquire()
            permitAcquired = true
            currentInflight.incrementAndGet()

            val transactionId = UUID.randomUUID()

            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            performRequestWithRetryAsync(paymentId, amount, transactionId, deadline, 1)
                .whenComplete { _, throwable ->
                    releaseInflightPermit()

                    if (throwable != null) {
                        logger.error("[$accountName] Async payment processing failed for payment $paymentId", throwable)
                        resultFuture.completeExceptionally(throwable)
                    } else {
                        resultFuture.complete(null)
                    }
                }

        } catch (e: Exception) {
            if (permitAcquired) {
                releaseInflightPermit()
            }
            logger.error("[$accountName] Error initiating payment $paymentId", e)
            resultFuture.completeExceptionally(e)
        }

        return resultFuture
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
        if (remaining <= 200 || attempt > maxAttemptAmount) {
            future.complete(null)
            return future
        }

        val histP95 = quantile(0.95)
        val avgMs = requestAverageProcessingTime.toMillis().coerceAtLeast(1L)
        val firstAttemptMinTimeout = (avgMs * 4).coerceAtLeast(100L).coerceAtMost(15_000L)
        val retryMinTimeout = (avgMs * 6).coerceAtLeast(150L).coerceAtMost(20_000L)
        val attemptTimeout = if (attempt == 1) {
            histP95.coerceAtLeast(firstAttemptMinTimeout).coerceAtMost(25_000L).coerceAtMost(remaining - 100)
        } else {
            histP95.coerceAtLeast(retryMinTimeout).coerceAtMost(40_000L).coerceAtMost(remaining - 100)
        }
        if (attemptTimeout <= 0L) {
            future.complete(null)
            return future
        }

        if (attempt > 1) {
            retryCounter.increment()
            logger.info("[$accountName] Retry #$attempt for payment $paymentId")
        }

        // Неблокирующее ограничение по rate limit:
        // если окно переполнено, планируем повторную попытку через небольшой backoff,
        // не занимая текущий поток ожиданием.
        if (!slidingWindowRateLimiter.tick()) {
            val timeToDeadline = deadline - now()
            if (attempt < maxAttemptAmount && timeToDeadline > minTimeToMakeRequest) {
                scheduleRetry(paymentId, amount, transactionId, deadline, attempt, future)
            } else {
                future.complete(null)
            }
            return future
        }

        val requestStartTime = now()
        val attemptStartTime = now()


        val clientWithTimeout = client.newBuilder()
            .callTimeout(Duration.ofMillis(attemptTimeout))
            .readTimeout(Duration.ofMillis(attemptTimeout))
            .build()

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        clientWithTimeout.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                val duration = now() - attemptStartTime
                if (!lastDurations.offerLast(duration)) {
                    lastDurations.pollFirst()
                    lastDurations.offerLast(duration)
                }

                if (e is SocketTimeoutException || e.cause is SocketTimeoutException) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), null, reason = "Request timeout")
                    }
                } else {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), null, reason = e.message ?: "unknown")
                    }
                }

                if (attempt < maxAttemptAmount && deadline - now() > minTimeToMakeRequest) {
                    scheduleRetry(paymentId, amount, transactionId, deadline, attempt, future)
                } else {
                    future.complete(null)
                }
            }

            override fun onResponse(call: Call, response: Response) {
                response.use {
                    val duration = now() - attemptStartTime
                    if (!lastDurations.offerLast(duration)) {
                        lastDurations.pollFirst()
                        lastDurations.offerLast(duration)
                    }

                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    counter.increment()

                    summary.record((now() - requestStartTime) / 1000.0)

                    val timeToDeadline = deadline - now()
                    if (body.result) {
                        future.complete(null)
                    } else if (attempt < maxAttemptAmount && timeToDeadline > minTimeToMakeRequest) {
                        scheduleRetry(paymentId, amount, transactionId, deadline, attempt, future)
                    } else {
                        future.complete(null)
                    }
                }
            }
        })

        return future
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long,
        attempt: Int,
        parentFuture: CompletableFuture<Void>
    ) {
        val adder = ThreadLocalRandom.current().nextLong(0, 50)
        val avgMs = requestAverageProcessingTime.toMillis().coerceAtLeast(1L)
        val baseBackoff = (avgMs / 2).coerceAtLeast(20L).coerceAtMost(500L)
        val backoff = (baseBackoff * (1L shl (attempt - 1))).coerceAtMost(1000L)
        val beforeDeadline = deadline - now()

        if (beforeDeadline <= 60) {
            parentFuture.complete(null)
            return
        }

        val actualSleep = minOf(backoff + adder, beforeDeadline - 60)
        if (actualSleep <= 0) {
            parentFuture.complete(null)
            return
        }

        CompletableFuture.delayedExecutor(actualSleep, TimeUnit.MILLISECONDS).execute {
            performRequestWithRetryAsync(paymentId, amount, transactionId, deadline, attempt + 1).whenComplete { _, throwable ->
                if (throwable != null) {
                    parentFuture.completeExceptionally(throwable)
                } else {
                    parentFuture.complete(null)
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()