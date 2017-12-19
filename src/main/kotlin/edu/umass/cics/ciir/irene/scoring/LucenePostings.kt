package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.chai.ComputedStats
import edu.umass.cics.ciir.chai.IntList
import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.index.PostingsEnum
import org.apache.lucene.index.Term
import org.apache.lucene.search.Explanation

/**
 * Created from [TextExpr] via [exprToEval]
 * @author jfoley.
 */
data class LuceneMissingTerm(val term: Term) : PositionsEvalNode, LeafEvalNode() {
    override fun positions(): PositionsIter = error("Don't ask for positions if count is zero!")
    override fun count() = 0
    override fun matches() = false
    override fun explain() = Explanation.match(0.0f, "MissingTerm-$term")
    override fun estimateDF() = 0L
}

data class LuceneDocLengths(val field: String, val lengths: NumericDocValues, val info: ComputedStats): CountEvalNode, LeafEvalNode() {
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
    override fun explain(): Explanation = Explanation.match(count().toFloat(), "lengths.$field $info")
    override fun estimateDF(): Long = lengths.cost()
    override fun count(): Int {
        if (matches()) {
            return lengths.longValue().toInt()
        }
        return 0
    }
}

abstract class LuceneTermFeature(val term: Term, val postings: PostingsEnum) : LeafEvalNode() {
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
        return "${this.javaClass.simpleName}($term)"
    }
    override fun estimateDF(): Long = postings.cost()
}

open class LuceneTermDocs(term: Term, postings: PostingsEnum) : LuceneTermFeature(term, postings) {
    override fun score(): Double = count().toDouble()
    override fun count(): Int = if (matches()) 1 else 0
}
open class LuceneTermCounts(term: Term, postings: PostingsEnum) : LuceneTermDocs(term, postings), CountEvalNode {
    override fun score(): Double = count().toDouble()
    override fun count(): Int {
        if(matches()) {
            return postings.freq()
        }
        return 0
    }
}
class LuceneTermPositions(term: Term, postings: PostingsEnum) : LuceneTermCounts(term, postings), PositionsEvalNode {
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
            return Explanation.match(count().toFloat(), "@doc=${env.doc}, positions=${positions()}")
        } else {
            return Explanation.noMatch("@doc=${postings.docID()} doc=${env.doc}, positions=[]")
        }
    }
}

