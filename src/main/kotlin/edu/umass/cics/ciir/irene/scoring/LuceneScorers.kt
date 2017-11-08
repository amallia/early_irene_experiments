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

data class IQContext(val index: IreneIndex, val context: LeafReaderContext) {
    val searcher = index.searcher
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
}

private class IQModelWeight(val q: QExpr, val iqm: IreneQueryModel) : Weight(iqm) {
    override fun extractTerms(terms: MutableSet<Term>?) {
        TODO("not implemented")
    }

    override fun explain(context: LeafReaderContext?, doc: Int): Explanation {
        val ctx = IQContext(iqm.index, context!!)
        return exprToEval(q, ctx).explain(doc)
    }

    override fun scorer(context: LeafReaderContext?): Scorer {
        val ctx = IQContext(iqm.index, context!!)
        return IreneQueryScorer(exprToEval(q, ctx), ctx)
    }
}

class QueryEvalNodeIter(val node: QueryEvalNode) : DocIdSetIterator() {
    init {
        //node.syncTo(0)
        //nextMatching(0)
    }
    fun nextMatching(doc: Int): Int {
        var id = doc
        while(id < NO_MORE_DOCS) {
            if (node.matches(id)) {
                return id
            }
            id = node.nextDoc()
        }
        return NO_MORE_DOCS
    }
    override fun advance(target: Int): Int = nextMatching(target)
    override fun nextDoc(): Int = node.advance(docID() + 1)
    override fun docID() = node.docID()
    fun score(doc: Int): Float {
        node.syncTo(doc)
        return node.score(doc)
    }
    fun count(doc: Int): Int {
        node.syncTo(doc)
        return node.count(doc)
    }
    fun matches(doc: Int): Boolean {
        node.syncTo(doc)
        return node.matches(doc)
    }
    fun explain(doc: Int): Explanation {
        node.syncTo(doc)
        return node.explain(doc)
    }
    fun estimateDF(): Long = node.estimateDF()
    override fun cost(): Long = node.estimateDF()
}

class IreneQueryScorer(val eval: QueryEvalNode, val ctx: IQContext) : Scorer(null) {
    val iter = QueryEvalNodeIter(eval)
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

class IreneQueryModel(val index: IreneIndex, val env: IreneQueryLanguage, val q: QExpr) : LuceneQuery() {
    val exec = env.prepare(index, q)

    override fun createWeight(searcher: IndexSearcher?, needsScores: Boolean, boost: Float): Weight {
        return IQModelWeight(exec, this)
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

    fun findFieldsNeeded(): Set<String> {
        val out = HashSet<String>()
        exec.visit {
            if (it is TextExpr) {
                out.add(it.field!!)
            }
        }
        return out
    }
}

