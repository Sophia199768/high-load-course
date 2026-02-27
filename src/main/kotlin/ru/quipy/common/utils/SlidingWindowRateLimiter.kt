package ru.quipy.common.utils

import java.time.Duration
import java.util.ArrayDeque
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    private val window: Duration,
) : RateLimiter {
    private val windowNanos = window.toNanos().coerceAtLeast(1L)
    private val timestampsNanos = ArrayDeque<Long>()
    private val lock = ReentrantLock()
    private val schedulerThreads = Runtime.getRuntime().availableProcessors().coerceIn(2, 8)
    private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(schedulerThreads)

    override fun tick(): Boolean {
        if (rate <= 0L) {
            return false
        }
        return lock.withLock {
            val now = System.nanoTime()
            cleanupExpired(now)
            if (timestampsNanos.size < rate.toInt()) {
                timestampsNanos.addLast(now)
                true
            } else {
                false
            }
        }
    }

    fun tickAsync(): CompletableFuture<Boolean> {
        if (tick()) {
            return CompletableFuture.completedFuture(true)
        }
        if (rate <= 0L) {
            return CompletableFuture.completedFuture(false)
        }

        val delayedResult = CompletableFuture<Boolean>()
        val delayNanos = lock.withLock {
            val now = System.nanoTime()
            cleanupExpired(now)
            if (timestampsNanos.size < rate.toInt()) {
                timestampsNanos.addLast(now)
                delayedResult.complete(true)
                0L
            } else {
                val oldest = timestampsNanos.first()
                (oldest + windowNanos - now).coerceAtLeast(1L)
            }
        }

        if (delayedResult.isDone) {
            return delayedResult
        }

        scheduler.schedule(
            {
                delayedResult.complete(tick())
            },
            delayNanos,
            TimeUnit.NANOSECONDS
        )
        return delayedResult
    }

    private fun cleanupExpired(nowNanos: Long) {
        while (timestampsNanos.isNotEmpty() && nowNanos - timestampsNanos.first() >= windowNanos) {
            timestampsNanos.removeFirst()
        }
    }
}