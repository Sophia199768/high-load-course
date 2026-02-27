package ru.quipy.common.utils

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger

class OngoingWindow(
    maxWinSize: Int
) {
    private val window = Semaphore(maxWinSize, true)

    fun acquire() {
        window.acquire()
    }

    fun release() = window.release()

    fun awaitingQueueSize() = window.queueLength
}

class NonBlockingOngoingWindow(
    private val maxWinSize: Int
) {
    private val permits = Semaphore(maxWinSize, false)
    private val winSize = AtomicInteger(0)

    fun putIntoWindow(): WindowResponse {
        if (!permits.tryAcquire()) {
            return WindowResponse.Fail(winSize.get())
        }
        return WindowResponse.Success(winSize.incrementAndGet())
    }

    fun releaseWindow(): Int {
        while (true) {
            val current = winSize.get()
            if (current <= 0) {
                return 0
            }
            if (winSize.compareAndSet(current, current - 1)) {
                permits.release()
                return current - 1
            }
        }
    }


    sealed class WindowResponse(val currentWinSize: Int) {
        public class Success(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)

        public class Fail(
            currentWinSize: Int
        ) : WindowResponse(currentWinSize)
    }
}