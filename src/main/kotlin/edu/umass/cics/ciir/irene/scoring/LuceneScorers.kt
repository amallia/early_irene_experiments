package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.chai.IntList
import edu.umass.cics.ciir.irene.DataNeeded
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.createOptimizedMovementExpr
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.lucene_try
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
    val evalCache = HashMap<Term, QueryEvalNode>()

    private fun getLengths(field: String) = lengths.computeIfAbsent(field, { missing ->
        // Get explicitly indexed lengths:
        lucene_try { context.reader().getNumericDocValues("lengths:$missing") }
                // Or get the norms... lucene doesn't always put lengths here...
                ?: lucene_try { context.reader().getNormValues(missing) }
                // Or crash.
                ?: error("Couldn't find norms for ``$missing'' ND=${context.reader().numDocs()} F=${context.reader().fieldInfos.map { it.name }}.")
    })

    fun create(term: Term, needed: DataNeeded): QueryEvalNode {
        return create(term, needed, getLengths(term.field()))
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

    private fun termRaw(term: Term, needed: DataNeeded, lengths: NumericDocValues): QueryEvalNode {
        val postings = termSeek(term, needed)
                ?: return LuceneMissingTerm(term)
        return when(needed) {
            DataNeeded.DOCS -> LuceneTermDocs(term, postings)
            DataNeeded.COUNTS -> LuceneTermCounts(term, postings)
            DataNeeded.POSITIONS -> LuceneTermPositions(term, postings)
            DataNeeded.SCORES -> error("Impossible!")
        }
    }

    private fun termCached(term: Term, needed: DataNeeded, lengths: NumericDocValues): QueryEvalNode {
        return if (env.shareIterators) {
            val entry = iterNeeds[term]
            if (entry == null || entry < needed) {
                error("Forgot to call .setup(q) on IQContext. While constructing $term $needed $entry...")
            }
            evalCache.computeIfAbsent(term, { termRaw(term, entry, lengths) })
        } else {
            termRaw(term, needed, lengths)
        }
    }
    fun termMover(term: Term): QueryMover {
        val result = termSeek(term, DataNeeded.DOCS) ?: return NeverMatchMover(term.toString())

        return LuceneDocsMover(term.toString(), result)
    }
    private fun termSeek(term: Term, needed: DataNeeded): PostingsEnum? {
        val termContext = TermContext.build(searcher.topReaderContext, term)!!
        val state = termContext[context.ord] ?: return null
        val termIter = context.reader().terms(term.field()).iterator()
        termIter.seekExact(term.bytes(), state)
        val postings = termIter.postings(null, needed.textFlags())
        // Lucene requires we call nextDoc() before doing anything else.
        postings.nextDoc()
        return postings
    }

    fun create(term: Term, needed: DataNeeded, lengths: NumericDocValues): QueryEvalNode {
        return termCached(term, needed, lengths)
    }
    fun shardIdentity(): Any = context.id()
    fun numDocs(): Int = context.reader().numDocs()

    fun createLengths(field: String): CountEvalNode {
        return LuceneDocLengths(field, getLengths(field))
    }

    fun setup(input: QExpr): QExpr {
        val foldOperators = if (!env.indexedBigrams) {
            input
        } else {
            input.map { q ->
                if (q is OrderedWindowExpr && q.children.size == 2) {
                    val lhs = q.children[0] as TextExpr
                    val rhs = q.children[1] as TextExpr
                    TextExpr("${lhs.text} ${rhs.text}", field="od:${lhs.countsField()}", statsField = "ERROR_PRECOMPUTED_STATS", needed = DataNeeded.COUNTS)
                } else q
            }
        }

        if (env.shareIterators) {
            foldOperators.findTextNodes().map { q ->
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

        return foldOperators
    }
}

private class IQModelWeight(val q: QExpr, val m: QExpr, val iqm: IreneQueryModel) : Weight(iqm) {
    override fun extractTerms(terms: MutableSet<Term>) {
        TODO("not implemented")
    }

    override fun explain(context: LeafReaderContext, doc: Int): Explanation {
        val ctx = IQContext(iqm, context)
        val rq = ctx.setup(q)
        val eval = exprToEval(rq, ctx)
        eval.setup(ScoringEnv(doc))
        return eval.explain()
    }

    override fun scorer(context: LeafReaderContext): Scorer {
        val ctx = IQContext(iqm, context)
        val rq = ctx.setup(q)
        val qExpr = exprToEval(rq, ctx)
        val mExpr = createMover(m, ctx)
        //println("mExpr: $mExpr")
        //println("qExpr: ${qExpr}")
        val iter = OptimizedMovementIter(mExpr, qExpr)
        return IreneQueryScorer(q, iter)
    }
}

class OptimizedMovementIter(val movement: QueryMover, val score: QueryEvalNode): DocIdSetIterator() {
    val env = score.setup(ScoringEnv())
    val firstMatch: Int = advance(0)
    override fun docID(): Int = movement.docID()
    override fun nextDoc(): Int = advance(docID()+1)
    override fun cost(): Long = movement.estimateDF()
    override fun advance(target: Int): Int {
        var dest = target
        while (!movement.done) {
            val nextMatch = movement.nextMatching(dest)
            if (nextMatch == NO_MORE_DOCS) break
            env.doc = nextMatch
            //println("Consider candidate: $nextMatch matching... ${score.matches()} ${score.explain()}")
            if (score.matches()) {
                return nextMatch
            } else {
                dest = maxOf(dest+1, nextMatch+1)
            }
        }
        return NO_MORE_DOCS
    }
    fun score(doc: Int): Float {
        env.doc = doc
        return score.score().toFloat()
    }
    fun count(doc: Int): Int {
        env.doc = doc
        return score.count()
    }
}

class IreneQueryScorer(val q: QExpr, val iter: OptimizedMovementIter) : Scorer(null) {
    val eval: QueryEvalNode = iter.score
    val env: ScoringEnv = iter.env
    override fun docID(): Int {
        val docid = iter.docID()
        //println(docid)
        return docid
    }
    override fun iterator(): DocIdSetIterator = iter
    override fun score(): Float {
        val returned = iter.score(docID())
        if (java.lang.Float.isInfinite(returned)) {
            throw RuntimeException("Infinite response for document: ${iter.docID()}@${env.doc} ${eval.explain()}")
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

