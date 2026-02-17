package ru.quipy.apigateway

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.TokenBucketRateLimiter

import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*
import java.util.concurrent.TimeUnit

@RestController
class APIController(
    @Autowired
    var registry: MeterRegistry
) {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer
    @Value("\${api.payment-rate-limit-enabled:true}")
    private var paymentRateLimitEnabled: Boolean = true
    @Value("\${api.payment-rate-limit-per-sec:5000}")
    private var paymentRateLimitPerSec: Int = 5000
    private lateinit var rateLimiter: TokenBucketRateLimiter
    private val counter = Counter.builder("queries.amount").tag("name", "orders").register(registry)
    private val counterPayment = Counter.builder("queries.amount").tag("name", "payment").register(registry)

    @PostConstruct
    fun initRateLimiter() {
        rateLimiter = TokenBucketRateLimiter(
            paymentRateLimitPerSec,
            paymentRateLimitPerSec,
            1,
            TimeUnit.SECONDS
        )
    }

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        counter.increment()
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        var save = orderRepository.save(order)
        return save
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<PaymentSubmissionDto> {
        val paymentId = UUID.randomUUID()

        val timestamp = System.currentTimeMillis() + 700
        if (paymentRateLimitEnabled && !rateLimiter.tick()) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", timestamp.toString())
                .build()
        }

        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        counterPayment.increment()

        return try {
            val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
            ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
        } catch (e: RuntimeException) {
            ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", (System.currentTimeMillis() + 300).toString())
                .build()
        }
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}