package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluators
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.NumericDocValues
import org.apache.lucene.index.Term
import org.apache.lucene.index.TermContext
import org.apache.lucene.search.*
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.SimpleEvalDoc
import java.io.File

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

class IQModelWeight(val q: QExpr, iqm: IreneQueryModel, val searcher: IndexSearcher) : Weight(iqm) {
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
    override fun score(): Float = eval.score(eval.docID())
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

fun exprToEval(q: QExpr, ctx: IQContext): QueryEvalNode = when(q) {
    is TextExpr -> ctx.create(Term(q.field, q.text), DataNeeded.COUNTS)
    is LuceneExpr -> TODO()
    is SynonymExpr -> TODO()
    is AndExpr -> TODO()
    is OrExpr -> TODO()
    is SumExpr -> SumEval(q.children.map { exprToEval(it, ctx) })
    is MeanExpr -> TODO()
    is MultExpr -> TODO()
    is MaxExpr -> TODO()
    is WeightExpr -> TODO()
    is DirQLExpr -> DirichletSmoothingEval(exprToEval(q.child, ctx) as CountEvalNode, q.mu!!)
    is BM25Expr -> TODO()
    is CountToScoreExpr -> TODO()
    is BoolToScoreExpr -> TODO()
    is CountToBoolExpr -> TODO()
    is RequireExpr -> TODO()
}

data class CountStats(val cf: Long, val df: Long, val cl: Long, val dc: Long) {
    fun avgDL() = cl.toDouble() / dc.toDouble();
    fun countProbability() = cf.toDouble() / cl.toDouble()
    fun binaryProbability() = df.toDouble() / dc.toDouble()
}

abstract class QueryEvalNode : DocIdSetIterator() {
    abstract fun score(doc: Int): Float
    abstract fun count(doc: Int): Int
    abstract fun matches(doc: Int): Boolean
    abstract fun explain(doc: Int): Explanation
    abstract fun estimateDF(): Long
    override fun cost(): Long = estimateDF()
    override fun nextDoc(): Int = advance(docID() + 1)
    val done: Boolean get() = docID() == NO_MORE_DOCS
}
abstract class CountEvalNode : QueryEvalNode() {
    abstract fun getCountStats(): CountStats
    abstract fun length(doc: Int): Int
}
abstract class LeafEvalNode : CountEvalNode() {

}

abstract class RecursiveEval(val children: List<QueryEvalNode>) : QueryEvalNode() {
    val className = this.javaClass.simpleName
    val N = children.size
    override fun explain(doc: Int): Explanation {
        val expls = children.map { it.explain(doc) }
        if (matches(doc)) {
            return Explanation.match(score(doc), "$className.Match", expls)
        }
        return Explanation.noMatch("$className.Miss", expls)
    }
}
abstract class OrEval(children: List<QueryEvalNode>) : RecursiveEval(children) {
    private var current: Int = 0
    val cost = children.map { it.estimateDF() }.max() ?: 0L
    override fun docID(): Int = current
    override fun advance(target: Int): Int {
        if (children.any { it.done }) return NO_MORE_DOCS

        var nextMin = NO_MORE_DOCS
        children.forEach { child ->
            var where = child.docID()
            while (where < target) {
                where = child.advance(target)
                // be aggressive; "hit" may exist where match fails.
                if (where == NO_MORE_DOCS) return@forEach
                if (child.matches(where)) break
            }
            nextMin = minOf(nextMin, where)
            if (nextMin == target) return@forEach
        }
        current = nextMin
        return nextMin
    }
    override fun estimateDF(): Long = cost

    override fun matches(doc: Int): Boolean {
        if (current > doc) return false
        else if (current < doc) return advance(doc) == doc
        return current == doc
    }
}

class SumEval(children: List<QueryEvalNode>) : OrEval(children) {
    val numHits = children.map { it.estimateDF() }.max() ?: 0L
    override fun score(doc: Int): Float {
        var sum = 0f
        children.forEach {
            sum += it.score(doc)
        }
        return sum
    }
    override fun count(doc: Int): Int {
        var sum = 0
        children.forEach {
            sum += it.count(doc)
        }
        return sum
    }
    override fun estimateDF(): Long = numHits
}

abstract class SingleChildEval<out T : QueryEvalNode> : QueryEvalNode() {
    abstract val child: T
    override fun docID(): Int = child.docID()
    override fun advance(target: Int): Int = child.advance(target)
    override fun estimateDF(): Long = child.estimateDF()
    override fun matches(doc: Int): Boolean = child.matches(doc)
}

class DirichletSmoothingEval(override val child: CountEvalNode, val mu: Double) : SingleChildEval<CountEvalNode>() {
    val background = mu * child.getCountStats().countProbability()
    override fun score(doc: Int): Float {
        val c = child.count(doc).toDouble()
        val length = child.length(doc).toDouble()
        return Math.log((c+ background) / (length + mu)).toFloat()
    }
    override fun count(doc: Int): Int = TODO("not yet")
    override fun explain(doc: Int): Explanation = TODO("not yet")
}

fun main(args: Array<String>) {
    val dataset = DataPaths.Robust
    val qrels = dataset.getQueryJudgments()
    val queries = dataset.getTitleQueries()
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()

    IreneIndex(IndexParams().apply {
        withPath(File("robust.irene2"))
    }).use { index ->
        println(index.getStats(Term("body", "president")))

        queries.forEach { qid, qtext ->
            val q = SumExpr(index.analyzer.tokenize("body", qtext).map { DirQLExpr(TextExpr(it)) })

            val qmodel = IreneQueryModel(index, index.language, q)
            val topK = index.searcher.search(qmodel, 1000)
            val results = QueryResults(topK.scoreDocs.mapIndexed { i, sdoc ->
                val name = index.getField(sdoc.doc, "id")?.stringValue() ?: "null"
                SimpleEvalDoc(name, i+1, sdoc.score.toDouble())
            })

            val queryJudgments = qrels[qid]
            evals.forEach { measure, evalfn ->
                val score = try {
                    evalfn.evaluate(results, queryJudgments)
                } catch (npe: NullPointerException) {
                    System.err.println("NULL in eval...")
                    -Double.MAX_VALUE
                }
                ms.push("$measure.irene2", score)
            }

            println(ms.means())
        }
    }
    println(ms.means())
}