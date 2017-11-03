package edu.umass.cics.ciir.irene

import org.apache.lucene.index.*
import org.apache.lucene.search.Explanation

/**
 *
 * @author jfoley.
 */
enum class DataNeeded(val level: Int) {
    DOCS(0), COUNTS(1), POSITIONS(2);
    fun flags(): Int = when(this) {
        DataNeeded.DOCS -> PostingsEnum.NONE.toInt()
        DataNeeded.COUNTS -> PostingsEnum.FREQS.toInt()
        DataNeeded.POSITIONS -> PostingsEnum.ALL.toInt()
    }
    companion object {
        fun max(lhs: DataNeeded?, rhs: DataNeeded?): DataNeeded {
            val max = maxOf(lhs?.level ?: 0, rhs?.level ?: 0)
            return when(max) {
                0 -> DOCS
                1 -> COUNTS
                2 -> POSITIONS
                else -> error("Invalid Level Needed: max($lhs,$rhs)")
            }
        }
    }
}

data class TermRequest(val term: Term, val termContext: TermContext, val stats: CountStats, val needed: DataNeeded) {
    val lengths = HashMap<String, NumericDocValues>()
    fun create(ctx: LeafReaderContext): LeafEvalNode {
        return create(ctx, lengths.computeIfAbsent(term.field(), { field ->
            lucene_try { ctx.reader().getNormValues(field) } ?: error("Couldn't find norms for ``$field''.")
        }))
    }
    fun create(ctx: LeafReaderContext, lengths: NumericDocValues): LeafEvalNode {
        val state = termContext[ctx.ord] ?: return LuceneMissingTerm(term, stats, lengths)
        val termIter = ctx.reader().terms(term.field()).iterator()
        termIter.seekExact(term.bytes(), state)
        return LuceneTermPostings(stats, termIter.postings(null, needed.flags()), lengths)
    }
}

data class LuceneMissingTerm(val term: Term, val stats: CountStats, val lengths: NumericDocValues) : LeafEvalNode {
    override fun score(doc: Int) = 0f
    override fun count(doc: Int) = 0
    override fun matches(doc: Int) = false
    override fun explain(doc: Int) = Explanation.match(0.0f, "MissingTerm-$term")
    override fun estimateDF() = 0L
    override fun getCountStats(): CountStats = stats
    override fun length(doc: Int): Int {
        if (lengths.advanceExact(doc)) {
            return lengths.longValue().toInt()
        }
        return 0
    }
}

class LuceneTermPostings(val stats: CountStats, val postings: PostingsEnum, val lengths: NumericDocValues) : LeafEvalNode {
    override fun score(doc: Int): Float {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

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
