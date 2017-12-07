package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.chai.IntList
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.irene.lang.IreneQueryLanguage
import edu.umass.cics.ciir.irene.lang.LuceneQuery
import edu.umass.cics.ciir.irene.lang.QExpr
import org.apache.lucene.index.*
import org.apache.lucene.search.*
import java.util.*

/**
 *
 * @author jfoley.
 */

data class IQContext(val iqm: IreneQueryModel, val context: LeafReaderContext) {
    val env = iqm.env
    val searcher = iqm.index.searcher
    val lengths = HashMap<String, NumericDocValues>()
    val iterNeeds = HashMap<Term, DataNeeded>()
    val iterCache = HashMap<Term, Optional<PostingsEnum>>()

    private fun getLengths(field: String) = lengths.computeIfAbsent(field, { missing ->
        lucene_try { context.reader().getNormValues(missing) } ?: error("Couldn't find norms for ``$missing'' ND=${context.reader().numDocs()} F=${context.reader().fieldInfos.map { it.name }}.")
    })

    fun create(term: Term, needed: DataNeeded, stats: CountStats): QueryEvalNode {
        return create(term, needed, stats, getLengths(term.field()))
    }

    fun selectRelativeDocIds(ids: List<Int>): IntList {
        val base = context.docBase
        val limit = base + context.reader().numDocs()
        val output = IntList()
        ids.forEach { id ->
            if(id >= base && id < limit) {
                output.push(id-base)
            }
        }
        output.sort()
        return output
    }

    private fun termCached(term: Term, needed: DataNeeded): Optional<PostingsEnum> {
        if (env.shareIterators) {
            val entry = iterNeeds[term]
            if (entry == null || entry < needed) {
                error("Forgot to call .setup(q) on IQContext. While constructing $term $needed $entry...")
            }
            return iterCache.computeIfAbsent(term, { termSeek(term, entry) })
        } else {
            return termSeek(term, needed)
        }
    }
    fun termMover(term: Term): QueryMover {
        val result = termSeek(term, DataNeeded.DOCS)
        if (result.isPresent) {
            return LuceneDocsMover(term.toString(), result.get())
        }
        return NeverMatchMover(term.toString())
    }
    private fun termSeek(term: Term, needed: DataNeeded): Optional<PostingsEnum> {
        val termContext = TermContext.build(searcher.topReaderContext, term)!!
        val state = termContext[context.ord] ?: return Optional.empty()
        val termIter = context.reader().terms(term.field()).iterator()
        termIter.seekExact(term.bytes(), state)
        val postings = termIter.postings(null, needed.textFlags())
        // Lucene requires we call nextDoc() before doing anything else.
        postings.nextDoc()
        return Optional.ofNullable(postings)
    }

    fun create(term: Term, needed: DataNeeded, stats: CountStats, lengths: NumericDocValues): QueryEvalNode {
        val optPostings = termCached(term, needed)
        if (!optPostings.isPresent) {
            return LuceneMissingTerm(term, stats, lengths)
        }
        val postings = optPostings.get()
        //val termContext = TermContext.build(searcher.topReaderContext, term)!!
        //val state = termContext[context.ord] ?: return LuceneMissingTerm(term, stats, lengths)
        //val termIter = context.reader().terms(term.field()).iterator()
        //termIter.seekExact(term.bytes(), state)
        //val postings = termIter.iter(null, needed.textFlags())
        return when(needed) {
            DataNeeded.DOCS -> LuceneTermDocs(stats, postings)
            DataNeeded.COUNTS -> LuceneTermCounts(stats, postings, lengths)
            DataNeeded.POSITIONS -> LuceneTermPositions(stats, postings, lengths)
            DataNeeded.SCORES -> error("Impossible!")
        }
    }
    fun shardIdentity(): Any = context.id()
    fun numDocs(): Int = context.reader().numDocs()
    fun createLengths(field: String, countStats: CountStats): QueryEvalNode {
        return LuceneDocLengths(countStats, getLengths(field))
    }

    fun setup(input: QExpr) {
        if (env.shareIterators) {
            input.findTextNodes().map { q ->
                val term = Term(q.countsField(), q.text)
                val needed = q.needed
                iterNeeds.compute(term, { missing, prev ->
                    if (prev == null) {
                        needed
                    } else {
                        maxOf(prev, needed)
                    }
                })

            }
        }
    }
}

private class IQModelWeight(val q: QExpr, val m: QExpr, val iqm: IreneQueryModel) : Weight(iqm) {
    override fun extractTerms(terms: MutableSet<Term>) {
        TODO("not implemented")
    }

    override fun explain(context: LeafReaderContext, doc: Int): Explanation {
        val ctx = IQContext(iqm, context)
        ctx.setup(q)
        return exprToEval(q, ctx).explain(doc)
    }

    override fun scorer(context: LeafReaderContext): Scorer {
        val ctx = IQContext(iqm, context)
        ctx.setup(q)
        val qExpr = exprToEval(q, ctx)
        val mExpr = createMover(m, ctx)
        val iter = OptimizedMovementIter(mExpr, qExpr)
        return IreneQueryScorer(q, iter)
    }
}

class OptimizedMovementIter(val movement: QueryMover, val score: QueryEvalNode): DocIdSetIterator() {
    val firstMatch: Int = advance(0)
    override fun docID(): Int = movement.docID()
    override fun nextDoc(): Int = advance(docID()+1)
    override fun cost(): Long = movement.estimateDF()
    override fun advance(target: Int): Int {
        var dest = target
        while (!movement.done) {
            val nextMatch = movement.nextMatching(dest)
            if (nextMatch == NO_MORE_DOCS) break
            if (score.matches(nextMatch)) {
                return nextMatch
            } else {
                dest = maxOf(dest+1, nextMatch+1)
            }
        }
        return NO_MORE_DOCS
    }
    fun score(doc: Int): Float {
        return score.score(doc)
    }
    fun count(doc: Int): Int {
        return score.count(doc)
    }
}

class IreneQueryScorer(val q: QExpr, val iter: OptimizedMovementIter) : Scorer(null) {
    val eval: QueryEvalNode = iter.score
    override fun docID(): Int {
        val docid = iter.docID()
        //println(docid)
        return docid
    }
    override fun iterator(): DocIdSetIterator = iter
    override fun score(): Float {
        val returned = iter.score(docID())
        if (java.lang.Float.isInfinite(returned)) {
            throw RuntimeException("Infinite response for document: ${iter.docID()} ${eval.explain(iter.docID())}")
            //return -Float.MAX_VALUE
        }
        return returned
    }
    override fun freq(): Int = iter.count(docID())
}

class IreneQueryModel(val index: IreneIndex, val env: IreneQueryLanguage, q: QExpr) : LuceneQuery() {
    val exec = env.prepare(q)
    var movement = if (env.optimizeMovement) {
        val m = env.prepare(createOptimizedMovementExpr(exec))
        //println(m)
        m
    } else {
        exec
    }

    override fun createWeight(searcher: IndexSearcher?, needsScores: Boolean, boost: Float): Weight {
        return IQModelWeight(exec, movement, this)
    }
    override fun hashCode(): Int {
        return exec.hashCode()
    }
    override fun equals(other: Any?): Boolean {
        if (other is IreneQueryModel) {
            return exec == other.exec
        }
        return false
    }
    override fun toString(field: String?): String {
        return exec.toString()
    }
}

