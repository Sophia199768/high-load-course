package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.*
import kotlin.RuntimeException

@Service
class OrderPayer(
    private val registry: MeterRegistry,
) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val linkedBlockingQueue = 20_000

    private val paymentExecutor = ThreadPoolExecutor(
        400,
        2000,
        60L, TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(linkedBlockingQueue),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.CallerRunsPolicy()
    )
    private val queueSizeGauge = Gauge.builder("queue.size", paymentExecutor.queue) { it.size.toDouble() }
        .register(registry)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val remainingMs = deadline - createdAt
        if (remainingMs <= 300) {
            throw RuntimeException()
        }

        val queueSize = paymentExecutor.queue.size
        val activeThreads = paymentExecutor.activeCount
        val estimatedQueueDelayMs = if (activeThreads > 0) {
            (queueSize.toLong() * 50L)
        } else {
            0L
        }

        if (remainingMs < estimatedQueueDelayMs + 500L) {
            throw RuntimeException()
        }

        if (queueSize > 1000 || paymentExecutor.queue.remainingCapacity() <= 10) {
            throw RuntimeException()
        }

        try {
            paymentExecutor.execute {
                try {
                    val remainingBeforeSubmit = deadline - System.currentTimeMillis()
                    if (remainingBeforeSubmit <= 200) {
                        logger.warn("Payment $paymentId skipped - deadline too close before submit ($remainingBeforeSubmit ms)")
                        return@execute
                    }

                    val createdEvent = paymentESService.create { it.create(paymentId, orderId, amount) }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } catch (e: Exception) {
                    logger.error("Error with $paymentId", e)
                }
            }
        } catch (e: RejectedExecutionException) {
            throw RuntimeException()
        }
        return createdAt
    }
}