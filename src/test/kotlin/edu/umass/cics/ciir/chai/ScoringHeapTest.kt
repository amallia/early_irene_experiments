package edu.umass.cics.ciir.chai

import org.junit.Assert.assertEquals
import org.junit.Test

data class ScoredInt(override val score: Float, val item: Int): ScoredForHeap

/**
 * @author jfoley
 */
class ScoringHeapTest {
    @Test
    fun offer() {
        val heap = ScoringHeap<ScoredInt>(5)

        (0 until 30).toList().shuffled().forEach { num ->
            val score = num / 30f
            heap.offer(score, {ScoredInt(score, num)})
        }

        assertEquals(listOf(29,28,27,26,25), heap.sorted.map { it.item })
    }

}