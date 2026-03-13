package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val account = checkAccount()

        if (account == null) {
            logger.warn("No available account for payment $paymentId — all circuit breakers OPEN or no time left")
            return
        }

        account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
    }

    private fun checkAccount(): PaymentExternalSystemAdapter? {
        val account = paymentAccounts.firstOrNull { it.isEnabled() } ?: return null

        return if (account.isAvailable()) {
            account
        } else {
            logger.warn("Circuit breaker OPEN")
            account
        }
    }
}
