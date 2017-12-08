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
data class LuceneMissingTerm(val term: Term, val stats: CountStats, val lengths: NumericDocValues) : PositionsEvalNode, QueryEvalNode {
    override fun positions(doc: ScoringEnv): PositionsIter = error("Don't ask for positions if count is zero!")
    override fun count(doc: ScoringEnv) = 0
    override fun matches(doc: ScoringEnv) = false
    override fun explain(doc: ScoringEnv) = Explanation.match(0.0f, "MissingTerm-$term length=${length(doc)}")
    override fun estimateDF() = 0L
    override fun getCountStats(): CountStats = stats
    override fun length(env: ScoringEnv): Int {
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

data class LuceneDocLengths(val stats: CountStats, val lengths: NumericDocValues): CountEvalNode {
    override fun count(doc: ScoringEnv): Int = 0
    override fun matches(doc: ScoringEnv): Boolean = lengths.docID() == doc.doc
    override fun explain(doc: ScoringEnv): Explanation = Explanation.match(length(doc).toFloat(), "lengths: $stats")
    override fun estimateDF(): Long = lengths.cost()
    override fun getCountStats(): CountStats = stats
    override fun length(env: ScoringEnv): Int {
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

abstract class LuceneTermFeature(val stats: CountStats, val postings: PostingsEnum) : QueryEvalNode {
    fun docID(): Int = postings.docID()
    fun syncTo(target: Int): Int {
        if (postings.docID() < target) {
            return postings.advance(target)
        }
        return postings.docID()
    }

    override fun matches(env: ScoringEnv): Boolean {
        val doc = env.doc
        syncTo(doc)
        return docID() == doc
    }

    override fun explain(doc: ScoringEnv): Explanation {
        if (matches(doc)) {
            return Explanation.match(count(doc).toFloat(), "${this.javaClass.simpleName} @doc=$doc")
        } else {
            return Explanation.noMatch("${this.javaClass.simpleName} @doc=${postings.docID()} doc=$doc")
        }
    }

    override fun estimateDF(): Long = stats.df
}

open class LuceneTermDocs(stats: CountStats, postings: PostingsEnum) : LuceneTermFeature(stats, postings) {
    override fun score(doc: ScoringEnv): Float = count(doc).toFloat()
    override fun count(doc: ScoringEnv): Int = if (matches(doc)) 1 else 0
}
open class LuceneTermCounts(stats: CountStats, postings: PostingsEnum, val lengths: NumericDocValues) : LuceneTermDocs(stats, postings), CountEvalNode {
    override fun score(doc: ScoringEnv): Float = count(doc).toFloat()
    override fun count(doc: ScoringEnv): Int {
        if(matches(doc)) {
            return postings.freq()
        }
        return 0
    }
    override fun getCountStats(): CountStats = stats
    override fun length(env: ScoringEnv): Int {
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
    override fun positions(env: ScoringEnv): PositionsIter {
        val doc = env.doc
        assert(postings.docID() != NO_MORE_DOCS) { "Requested positions from term that is finished!" }
        if (posDoc != doc) {
            posDoc = doc
            positions.clear()
            val count = count(env)
            if (count == 0) error("Don't ask for positions when count is zero.")

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

    override fun explain(doc: ScoringEnv): Explanation {
        if (matches(doc)) {
            return Explanation.match(count(doc).toFloat(), "@doc=$doc, lengths@${lengths.docID()} positions=${positions(doc)}")
        } else {
            return Explanation.noMatch("@doc=${postings.docID()} doc=$doc, lengths@=${lengths.docID()} positions=[]")
        }
    }
}

