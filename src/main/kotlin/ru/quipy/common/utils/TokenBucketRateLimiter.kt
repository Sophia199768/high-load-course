package ru.quipy.common.utils

import java.util.concurrent.TimeUnit

class TokenBucketRateLimiter(
    private val rate: Int,
    private val bucketMaxCapacity: Int,
    private val window: Long,
    private val timeUnit: TimeUnit = TimeUnit.MINUTES,
): RateLimiter {
    private val capacity = bucketMaxCapacity.coerceAtLeast(1)
    private val refillRatePerNano = rate.toDouble() / timeUnit.toNanos(window.coerceAtLeast(1L)).toDouble()
    private var availableTokens = capacity.toDouble()
    private var lastRefillNanos = System.nanoTime()

    override fun tick(): Boolean {
        if (rate <= 0) {
            return false
        }
        synchronized(this) {
            refill()
            if (availableTokens < 1.0) {
                return false
            }
            availableTokens -= 1.0
            return true
        }
    }

    private fun refill() {
        val now = System.nanoTime()
        val elapsed = (now - lastRefillNanos).coerceAtLeast(0L)
        if (elapsed > 0L) {
            availableTokens = (availableTokens + elapsed * refillRatePerNano).coerceAtMost(capacity.toDouble())
            lastRefillNanos = now
        }
    }
}