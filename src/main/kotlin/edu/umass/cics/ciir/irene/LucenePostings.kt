package edu.umass.cics.ciir.irene

import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.index.PostingsEnum
import org.apache.lucene.index.Term
import org.apache.lucene.search.Explanation

/**
 *
 * @author jfoley.
 */

data class LuceneMissingTerm(val term: Term, val stats: CountStats, val lengths: NumericDocValues) : LeafEvalNode() {
    override fun docID(): Int = NO_MORE_DOCS
    override fun advance(target: Int): Int = NO_MORE_DOCS
    override fun score(doc: Int) = 0f
    override fun count(doc: Int) = 0
    override fun matches(doc: Int) = false
    override fun explain(doc: Int) = Explanation.match(0.0f, "MissingTerm-$term length=${length(doc)}")
    override fun estimateDF() = 0L
    override fun getCountStats(): CountStats = stats
    override fun length(doc: Int): Int {
        if (lengths.advanceExact(doc)) {
            return lengths.longValue().toInt()
        }
        return 0
    }
}

class LuceneTermPostings(val stats: CountStats, val postings: PostingsEnum, val lengths: NumericDocValues) : LeafEvalNode() {
    override fun docID(): Int = postings.docID()
    override fun advance(target: Int): Int = postings.advance(target)
    override fun score(doc: Int): Float = count(doc).toFloat()
    override fun count(doc: Int): Int {
        if(matches(doc)) {
            return postings.freq()
        }
        return 0
    }

    override fun matches(doc: Int): Boolean {
        val current = postings.docID()
        if (current > doc) return false
        else if (current == doc) return true
        return doc == postings.advance(doc)
    }

    override fun explain(doc: Int): Explanation {
        if (matches(doc)) {
            return Explanation.match(count(doc).toFloat(), "@doc=$doc")
        } else {
            return Explanation.noMatch("@doc=${postings.docID()} doc=$doc")
        }
    }

    override fun estimateDF(): Long = stats.df
    override fun getCountStats(): CountStats = stats
    override fun length(doc: Int): Int {
        if (lengths.advanceExact(doc)) {
            return lengths.longValue().toInt()
        }
        return 0
    }

}
