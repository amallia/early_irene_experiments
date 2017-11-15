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

data class IQContext(val iqm: IreneQueryModel, val context: LeafReaderContext) {
    val env = iqm.env
    val searcher = iqm.index.searcher
    val lengths = HashMap<String, NumericDocValues>()

    fun create(term: Term, needed: DataNeeded, stats: CountStats): QueryEvalNode {
        return create(term, needed, stats, lengths.computeIfAbsent(term.field(), { field ->
            lucene_try { context.reader().getNormValues(field) } ?: error("Couldn't find norms for ``$field''.")
        }))
    }
    fun create(term: Term, needed: DataNeeded, stats: CountStats, lengths: NumericDocValues): QueryEvalNode {
        val termContext = TermContext.build(searcher.topReaderContext, term)!!
        val state = termContext[context.ord] ?: return LuceneMissingTerm(term, stats, lengths)
        val termIter = context.reader().terms(term.field()).iterator()
        termIter.seekExact(term.bytes(), state)
        val postings = termIter.postings(null, needed.textFlags())
        return when(needed) {
            DataNeeded.DOCS -> LuceneTermDocs(stats, postings)
            DataNeeded.COUNTS -> LuceneTermCounts(stats, postings, lengths)
            DataNeeded.POSITIONS -> LuceneTermPositions(stats, postings, lengths)
            DataNeeded.SCORES -> error("Impossible!")
        }
    }
    fun numDocs(): Int = context.reader().numDocs()
}

private class IQModelWeight(val q: QExpr, val iqm: IreneQueryModel) : Weight(iqm) {
    override fun extractTerms(terms: MutableSet<Term>?) {
        TODO("not implemented")
    }

    override fun explain(context: LeafReaderContext?, doc: Int): Explanation {
        val ctx = IQContext(iqm, context!!)
        return exprToEval(q, ctx).explain(doc)
    }

    override fun scorer(context: LeafReaderContext?): Scorer {
        val ctx = IQContext(iqm, context!!)
        return IreneQueryScorer(exprToEval(q, ctx))
    }
}

class QueryEvalNodeIter(val node: QueryEvalNode) : DocIdSetIterator() {
    init {
        node.nextMatching(0)
    }
    override fun advance(target: Int): Int = node.nextMatching(target)
    override fun nextDoc(): Int = node.nextMatching(docID() + 1)
    override fun docID() = node.docID()
    fun score(doc: Int): Float {
        node.syncTo(doc)
        return node.score(doc)
    }
    fun count(doc: Int): Int {
        node.syncTo(doc)
        return node.count(doc)
    }
    override fun cost(): Long = node.estimateDF()
}

class IreneQueryScorer(val eval: QueryEvalNode) : Scorer(null) {
    private val iter = QueryEvalNodeIter(eval)
    override fun docID(): Int = eval.docID()
    override fun iterator(): DocIdSetIterator = iter
    override fun score(): Float {
        val returned = eval.score(eval.docID())
        if (java.lang.Float.isInfinite(returned)) {
            throw RuntimeException("Infinite response for document: ${eval.docID()} ${eval.explain(eval.docID())}")
            //return -Float.MAX_VALUE
        }
        return returned
    }
    override fun freq(): Int = eval.count(eval.docID())
}

class IreneQueryModel(val index: IreneIndex, val env: IreneQueryLanguage, q: QExpr) : LuceneQuery() {
    val exec = env.prepare(q)

    override fun createWeight(searcher: IndexSearcher?, needsScores: Boolean, boost: Float): Weight {
        return IQModelWeight(exec, this)
    }
    override fun hashCode(): Int {
        return exec.hashCode()
    }
    override fun equals(other: Any?): Boolean {
        if (other is IreneQueryModel) {
            return exec.equals(other.exec)
        }
        return false
    }
    override fun toString(field: String?): String {
        return exec.toString()
    }
}

