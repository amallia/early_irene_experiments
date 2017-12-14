package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.chai.IntList
import edu.umass.cics.ciir.irene.CountStats
import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.index.PostingsEnum
import org.apache.lucene.index.Term
import org.apache.lucene.search.Explanation

/**
 * Created from [TextExpr] via [exprToEval]
 * @author jfoley.
 */
data class LuceneMissingTerm(val term: Term, val stats: CountStats, val lengths: NumericDocValues) : PositionsEvalNode, LeafEvalNode() {
    override fun positions(): PositionsIter = error("Don't ask for positions if count is zero!")
    override fun count() = 0
    override fun matches() = false
    override fun explain() = Explanation.match(0.0f, "MissingTerm-$term length=${length()}")
    override fun estimateDF() = 0L
    override fun getCountStats(): CountStats = stats
    override fun length(): Int {
        val doc = env.doc
        if (lengths.docID() < doc) {
            lengths.advance(doc)
        }
        if (lengths.docID() == doc) {
            return lengths.longValue().toInt()
        }
        return 0
    }
}

data class LuceneDocLengths(val stats: CountStats, val lengths: NumericDocValues): CountEvalNode, LeafEvalNode() {
    override fun count(): Int = length()
    override fun matches(): Boolean {
        val doc = env.doc
        if (lengths.docID() < doc) {
            lengths.advance(doc)
        }
        if (lengths.docID() == doc) {
            return true
        }
        return false
    }
    override fun explain(): Explanation = Explanation.match(length().toFloat(), "lengths: $stats")
    override fun estimateDF(): Long = lengths.cost()
    override fun getCountStats(): CountStats = stats
    override fun length(): Int {
        if (matches()) {
            return lengths.longValue().toInt()
        }
        return 0
    }
}

abstract class LuceneTermFeature(val stats: CountStats, val postings: PostingsEnum) : LeafEvalNode() {
    fun docID(): Int = postings.docID()
    fun syncTo(target: Int): Int {
        if (postings.docID() < target) {
            return postings.advance(target)
        }
        return postings.docID()
    }

    override fun matches(): Boolean {
        val doc = env.doc
        syncTo(doc)
        return docID() == doc
    }

    override fun explain(): Explanation {
        if (matches()) {
            return Explanation.match(count().toFloat(), "${this.javaClass.simpleName} @doc=${env.doc}")
        } else {
            return Explanation.noMatch("${this.javaClass.simpleName} @doc=${postings.docID()} doc=${env.doc}")
        }
    }

    override fun toString(): String {
        return "${this.javaClass.simpleName}(${stats.text})"
    }

    override fun estimateDF(): Long = stats.df
}

open class LuceneTermDocs(stats: CountStats, postings: PostingsEnum) : LuceneTermFeature(stats, postings) {
    override fun score(): Double = count().toDouble()
    override fun count(): Int = if (matches()) 1 else 0
}
open class LuceneTermCounts(stats: CountStats, postings: PostingsEnum, val lengths: NumericDocValues) : LuceneTermDocs(stats, postings), CountEvalNode {
    override fun score(): Double = count().toDouble()
    override fun count(): Int {
        if(matches()) {
            return postings.freq()
        }
        return 0
    }
    override fun getCountStats(): CountStats = stats
    override fun length(): Int {
        val doc = env.doc
        if (lengths.docID() < doc) {
            lengths.advance(doc)
        }
        if (lengths.docID() == doc) {
            return lengths.longValue().toInt()
        }
        return 0;
    }
}
class LuceneTermPositions(stats: CountStats, postings: PostingsEnum, lengths: NumericDocValues) : LuceneTermCounts(stats, postings, lengths), PositionsEvalNode {
    var posDoc = -1
    var positions = IntList()
    override fun positions(): PositionsIter {
        val doc = env.doc
        syncTo(doc)
        assert(postings.docID() != NO_MORE_DOCS) { "Requested positions from term that is finished!" }
        assert(postings.docID() == doc)
        if (posDoc != doc) {
            posDoc = doc
            positions.clear()
            val count = count()
            if (count == 0) error("Don't ask for positions when count is zero.")

            positions.reserve(count)
            (0 until count).forEach {
                try {
                    positions.push(postings.nextPosition())
                } catch (aerr: AssertionError) {
                    println("ASSERTION: $aerr")
                    throw aerr;
                }
            }
        }
        return PositionsIter(positions.unsafeArray(), positions.fill)
    }

    override fun explain(): Explanation {
        if (matches()) {
            return Explanation.match(count().toFloat(), "@doc=${env.doc}, lengths@${lengths.docID()} positions=${positions()}")
        } else {
            return Explanation.noMatch("@doc=${postings.docID()} doc=${env.doc}, lengths@=${lengths.docID()} positions=[]")
        }
    }
}

