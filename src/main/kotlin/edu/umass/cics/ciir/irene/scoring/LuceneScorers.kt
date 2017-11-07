package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.*
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.index.Term
import org.apache.lucene.index.TermContext
import org.apache.lucene.search.*

/**
 *
 * @author jfoley.
 */

data class IQContext(val searcher: IndexSearcher, val context: LeafReaderContext) {
    val lengths = HashMap<String, NumericDocValues>()

    fun create(term: Term, needed: DataNeeded): LeafEvalNode {
        return create(term, needed, lengths.computeIfAbsent(term.field(), { field ->
            lucene_try { context.reader().getNormValues(field) } ?: error("Couldn't find norms for ``$field''.")
        }))
    }
    fun create(term: Term, needed: DataNeeded, lengths: NumericDocValues): LeafEvalNode {
        val termContext = TermContext.build(searcher.topReaderContext, term)!!
        val cstats = searcher.collectionStatistics(term.field())!!
        val termStats = searcher.termStatistics(term, termContext)!!
        val stats = CountStats(termStats.docFreq(), termStats.totalTermFreq(), cstats.sumTotalTermFreq(), cstats.docCount())
        val state = termContext[context.ord] ?: return LuceneMissingTerm(term, stats, lengths)
        val termIter = context.reader().terms(term.field()).iterator()
        termIter.seekExact(term.bytes(), state)
        return LuceneTermPostings(stats, termIter.postings(null, needed.flags()), lengths)
    }
}

private class IQModelWeight(val q: QExpr, iqm: IreneQueryModel, val searcher: IndexSearcher) : Weight(iqm) {
    override fun extractTerms(terms: MutableSet<Term>?) {
        TODO("not implemented")
    }

    override fun explain(context: LeafReaderContext?, doc: Int): Explanation {
        val ctx = IQContext(searcher, context!!)
        return exprToEval(q, ctx).explain(doc)
    }

    override fun scorer(context: LeafReaderContext?): Scorer {
        val ctx = IQContext(searcher, context!!)
        return IreneQueryScorer(exprToEval(q, ctx))
    }
}

class IreneQueryScorer(val eval: QueryEvalNode) : Scorer(null) {
    override fun docID(): Int = eval.docID()
    override fun iterator(): DocIdSetIterator = eval
    override fun score(): Float {
        val returned = eval.score(eval.docID())
        if (java.lang.Float.isInfinite(returned)) {
            throw RuntimeException("Infinite response for document: ${eval.docID()} ${eval.explain(eval.docID())}")
            return -Float.MAX_VALUE
        }
        return returned
    }
    override fun freq(): Int = eval.count(eval.docID())
}

class IreneQueryModel(val index: IreneIndex, val env: IreneQueryLanguage, val q: QExpr) : LuceneQuery() {
    val exec = env.prepare(index, q)

    override fun createWeight(searcher: IndexSearcher?, needsScores: Boolean, boost: Float): Weight {
        return IQModelWeight(exec, this, searcher!!)
    }
    override fun hashCode(): Int {
        return q.hashCode() + env.hashCode();
    }
    override fun equals(other: Any?): Boolean {
        if (other is IreneQueryModel) {
            return q.equals(other.q) && env.equals(other.env)
        }
        return false
    }
    override fun toString(field: String?): String {
        return q.toString()
    }
}
