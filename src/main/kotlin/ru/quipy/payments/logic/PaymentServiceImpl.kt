package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val rrIndex = AtomicInteger(0)

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val account = chooseAccount()

        if (account == null) {
            logger.warn("No available account for payment $paymentId — all circuit breakers OPEN or no time left")
            return
        }

        account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
    }

    private fun chooseAccount(): PaymentExternalSystemAdapter? {
        val enabled = paymentAccounts.filter { it.isEnabled() }

        if (enabled.isEmpty()) return null

        val available = enabled.filter { it.isAvailable() }
        val candidates = if (available.isNotEmpty()) available else { logger.warn("All circuit breakers OPEN, allowing one probe request")
            enabled
        }

        val minPrice = candidates.minOf { it.price() }
        val cheapest = candidates.filter { it.price() == minPrice }

        return cheapest[rrIndex.getAndIncrement() % cheapest.size]
    }
}
