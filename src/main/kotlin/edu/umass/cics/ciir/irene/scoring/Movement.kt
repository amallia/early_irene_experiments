package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.*
import org.apache.lucene.index.Term
import org.apache.lucene.search.DocIdSetIterator

fun createMover(q: QExpr, ctx: IQContext): QueryMover {
    val reified =  createMoverRec(q, ctx)

    // TODO, clean up optimizations based on actual leaf presence of terms.
    // OR(xs ... false) -> OR(xs)
    // OR(xs ... true) -> true
    // etc.

    return reified
}

private fun createMoverRec(q: QExpr, ctx: IQContext) : QueryMover = when(q) {
    // OR nodes:
    is SynonymExpr, is OrExpr, is CombineExpr, is MultExpr, is MaxExpr, is MultiExpr -> OrMover(q.children.map { createMoverRec(it, ctx) })

    // Leaves:
    is TextExpr -> ctx.termMover(Term(q.countsField(), q.text))
    is LuceneExpr -> TODO("LuceneExpr")
    is LengthsExpr, is ConstCountExpr, is ConstBoolExpr, is ConstScoreExpr -> AlwaysMatchMover(q.toString(), ctx.numDocs())

// AND nodes:
    is AndExpr, is MinCountExpr, is OrderedWindowExpr, is UnorderedWindowExpr -> AndMover(q.children.map { createMoverRec(it, ctx) })

// Transformers are straight-forward:
    is CountToScoreExpr, is BoolToScoreExpr, is CountToBoolExpr, is AbsoluteDiscountingQLExpr, is BM25Expr, is WeightExpr, is DirQLExpr -> createMoverRec(q.trySingleChild, ctx)

// NOTE: Galago semantics, only look at cond. This is not an AND like you might think.
    is RequireExpr -> createMoverRec(q.cond, ctx)
}

/** Borrow this constant locally. */
const val MOVER_DONE = DocIdSetIterator.NO_MORE_DOCS
interface QueryMover {
    fun docID(): Int
    fun matches(doc: Int): Boolean
    // Used to accelerate AND and OR matching if accurate.
    fun estimateDF(): Long
    // Move forward to (docID() >= target) don't bother checking for match.
    fun advance(target: Int): Int

    // The following movements should never be overridden.
    // They may be used as convenient to express other operators, i.e. call nextMatching on your children instead of advance().

    // Use advance to move forward until match.
    fun nextMatching(doc: Int): Int {
        var id = maxOf(doc, docID());
        while(id < MOVER_DONE) {
            if (matches(id)) {
                assert(docID() >= id)
                return id
            }
            id = advance(id+1)
        }
        return id;
    }
    // Step to the next matching document.
    fun nextDoc(): Int = nextMatching(docID()+1)
    // True if iterator is exhausted.
    val done: Boolean get() = docID() == MOVER_DONE
    // Safe interface to advance. Only moves forward if need be.
    fun syncTo(target: Int) {
        if (docID() < target) {
            advance(target)
            assert(docID() >= target)
        }
    }
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class AndMover(val children: List<QueryMover>) : QueryMover {
    val N = children.size
    private var current: Int = 0
    val cost = children.map { it.estimateDF() }.min() ?: 0L
    val moveChildren = children.sortedBy { it.estimateDF() }
    init {
        advanceToMatch()
    }
    override fun docID(): Int = current
    fun advanceToMatch(): Int {
        while(current < NO_MORE_DOCS) {
            var match = true
            for (child in moveChildren) {
                var pos = child.docID()
                if (pos < current) {
                    pos = child.nextMatching(current)
                }
                if (pos > current) {
                    current = pos
                    match = false
                    break
                }
            }

            if (match) return current
        }
        return current
    }

    override fun advance(target: Int): Int {
        if (current < target) {
            current = target
            return advanceToMatch()
        }
        return current
    }
    override fun estimateDF(): Long = cost

    override fun matches(doc: Int): Boolean {
        if (current > doc) return false
        if (current == doc) return true
        return advance(doc) == doc
    }
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class OrMover(val children: List<QueryMover>) : QueryMover {
    var current = children.map { it.nextMatching(0) }.min()!!
    val cost = children.map { it.estimateDF() }.max() ?: 0L
    val moveChildren = children.sortedByDescending { it.estimateDF() }
    override fun docID(): Int = current
    override fun advance(target: Int): Int {
        var newMin = NO_MORE_DOCS
        for (child in moveChildren) {
            var where = child.docID()
            if (where < target) {
                where = child.advance(target)
            }
            assert(where >= target)
            newMin = minOf(newMin, where)
        }
        current = newMin
        return current
    }
    override fun estimateDF(): Long = cost

    override fun matches(doc: Int): Boolean {
        syncTo(doc)
        assert(docID() >= doc)
        return children.any { it.matches(doc) }
    }
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class NeverMatchMover(val name: String) : QueryMover {
    override fun docID(): Int = MOVER_DONE
    override fun matches(doc: Int): Boolean = false
    override fun estimateDF(): Long = 0
    override fun advance(target: Int): Int = MOVER_DONE
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class AlwaysMatchMover(val name: String, val numDocs: Int): QueryMover {
    var current = 0
    override fun docID(): Int = current
    override fun matches(doc: Int): Boolean {
        current = maxOf(current, doc)
        return true
    }
    override fun estimateDF(): Long = numDocs.toLong()
    override fun advance(target: Int): Int {
        current = maxOf(target, current)
        return current
    }
}

/** Implements [QueryMover] which is basically a lucene document iterator */
data class LuceneDocsMover(val name: String, val iter: DocIdSetIterator) : QueryMover {
    override fun docID(): Int = iter.docID()
    override fun matches(doc: Int): Boolean {
        syncTo(doc)
        return docID() == doc
    }
    override fun estimateDF(): Long = iter.cost()
    override fun advance(target: Int): Int {
        // Advance in lucene is not safe if past the value, so don't just proxy.
        if (iter.docID() < target) {
            return iter.advance(target)
        }
        return iter.docID()
    }

}