package edu.umass.cics.ciir.irene.scoring

import org.junit.Assert.assertEquals
import org.junit.Test

class IntSetMover(xs: Collection<Int>): QueryMover {
    val ordered = (xs).sorted()
    var pos = 0
    override val done: Boolean get() = pos >= ordered.size
    override fun docID(): Int = if (done) NO_MORE_DOCS else ordered[pos]
    override fun matches(doc: Int): Boolean {
        syncTo(doc)
        if (done) return false
        return ordered[pos] == doc
    }
    override fun estimateDF(): Long = ordered.size.toLong()
    override fun advance(target: Int): Int {
        if (done) return NO_MORE_DOCS
        while(!done && ordered[pos] < target) {
            pos++
        }
        return docID()
    }
}

fun QueryMover.collectList(): List<Int> {
    val output = ArrayList<Int>(this.estimateDF().toInt())
    while(!this.done) {
        output.add(docID())
        nextDoc()
    }
    return output
}

/**
 * @author jfoley.
 */
class AndMoverTest {
    @Test
    fun testAndMovement() {
        val lhs = IntSetMover(listOf(1,2,7,8))
        val rhs = IntSetMover(listOf(1,2,8))

        val and = AndMover(listOf(lhs, rhs))
        assertEquals(listOf(1, 2,8), and.collectList())
    }

    @Test
    fun testAndMovementEmpty() {
        val lhs = IntSetMover(listOf(10,12,17,18))
        val rhs = IntSetMover(listOf(1,2,8))

        val and = AndMover(listOf(lhs, rhs))
        assertEquals(emptyList<Int>(), and.collectList())
    }
}