package edu.umass.cics.ciir.chai

import edu.umass.cics.ciir.irene.utils.ScoringHeap
import edu.umass.cics.ciir.irene.utils.WeightedForHeap
import edu.umass.cics.ciir.irene.utils.shuffled
import org.junit.Assert.assertEquals
import org.junit.Test

data class WeightedInt(override val weight: Float, val item: Int): WeightedForHeap

/**
 * @author jfoley
 */
class ScoringHeapTest {
    @Test
    fun offer() {
        val heap = ScoringHeap<WeightedInt>(5)

        (0 until 30).toList().shuffled().forEach { num ->
            val score = num / 30f
            heap.offer(score, { WeightedInt(score, num)})
        }

        assertEquals(listOf(29,28,27,26,25), heap.sorted.map { it.item })
    }

}