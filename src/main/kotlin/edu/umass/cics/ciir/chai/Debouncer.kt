package edu.umass.cics.ciir.chai

import java.time.Duration

/**
 *
 * @author jfoley.
 */
class Debouncer
/**
 * @param milliseconds the minimum number of milliseconds between actions.
 */
@JvmOverloads constructor(var delay: Long = 1000) {
    var startTime: Long = 0
    var lastTime: Long = 0

    val now: Long get() = System.currentTimeMillis()
    val spentTimePretty: String get() = prettyTimeOfMillis(System.currentTimeMillis() - startTime)

    init {
        this.startTime = now
        this.lastTime = startTime - delay
    }

    fun ready(): Boolean {
        val now = System.currentTimeMillis()
        if (now - lastTime >= delay) {
            lastTime = now
            return true
        }
        return false
    }

    fun estimate(currentItem: Long, totalItems: Long): RateEstimate {
        return RateEstimate(System.currentTimeMillis() - startTime, currentItem, totalItems)
    }

    class RateEstimate(
            /** time spent on job (s)  */
            private val time: Long, private val itemsComplete: Long, private val totalItems: Long) {
        /** fraction of job complete  */
        private val fraction: Double = itemsComplete / totalItems.toDouble()

        /** items/ms  */
        private fun getRate(): Double = itemsComplete / time.toDouble()
        /** estimated time remaining (ms)  */
        private val remaining: Double

        init {
            this.remaining = (totalItems - itemsComplete) / getRate()
        }

        fun itemsPerSecond(): Double {
            return getRate() * 1000.0
        }

        fun percentComplete(): Double {
            return fraction * 100.0
        }

        override fun toString(): String {
            return String.format("%d/%d items, %4.1f items/s [%s left]; [%s spent], %2.1f%% complete.", itemsComplete, totalItems, itemsPerSecond(), prettyTimeOfMillis(remaining.toLong()), prettyTimeOfMillis(time), percentComplete())
        }
    }

    companion object {

        fun prettyTimeOfMillis(millis: Long): String {
            val msg = StringBuilder()
            var dur = Duration.ofMillis(millis)
            val days = dur.toDays()
            if (days > 0) {
                dur = dur.minusDays(days)
                msg.append(days).append(" days")
            }
            val hours = dur.toHours()
            if (hours > 0) {
                dur = dur.minusHours(hours)
                if (msg.length > 0) msg.append(", ")
                msg.append(hours).append(" hours")
            }
            val minutes = dur.toMinutes()
            if (minutes > 0) {
                if (msg.length > 0) msg.append(", ")
                msg.append(minutes).append(" minutes")
            }
            dur = dur.minusMinutes(minutes)
            val seconds = dur.toMillis() / 1e3
            if (msg.length > 0) msg.append(", ")
            msg.append(String.format("%1.3f", seconds)).append(" seconds")
            return msg.toString()
        }
    }
}

