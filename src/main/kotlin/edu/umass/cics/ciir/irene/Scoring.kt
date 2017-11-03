package edu.umass.cics.ciir.irene

import org.apache.lucene.search.Explanation
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.search.Weight

/**
 *
 * @author jfoley.
 */

class IreneQueryModel(val index: IreneIndex, val env: IreneQueryLanguage, val q: QExpr) : LuceneQuery() {
    val exec = env.prepare(index, q)

    override fun createWeight(searcher: IndexSearcher?, needsScores: Boolean, boost: Float): Weight {
        return super.createWeight(searcher, needsScores, boost)
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

fun exprToEval(q: QExpr): QueryEvalNode = when(q) {
    is TextExpr -> TODO()
    is LuceneExpr -> TODO()
    is SynonymExpr -> TODO()
    is AndExpr -> TODO()
    is OrExpr -> TODO()
    is SumExpr -> TODO()
    is MeanExpr -> TODO()
    is MultExpr -> TODO()
    is MaxExpr -> TODO()
    is WeightExpr -> TODO()
    is DirQLExpr -> TODO()
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

interface QueryEvalNode {
    fun score(doc: Int): Float
    fun count(doc: Int): Int
    fun matches(doc: Int): Boolean
    fun explain(doc: Int): Explanation
    fun estimateDF(): Long
}
interface CountEvalNode : QueryEvalNode {
    fun getCountStats(): CountStats
    fun length(doc: Int): Int
}
interface LeafEvalNode : CountEvalNode {

}

abstract class RecursiveEval(val children: List<QueryEvalNode>) : QueryEvalNode {
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
class SumEval(children: List<QueryEvalNode>) : RecursiveEval(children) {
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

    override fun matches(doc: Int): Boolean {
        children.forEach {
            if (it.matches(doc)) return true
        }
        return false
    }

    override fun estimateDF(): Long = numHits
}

abstract class SingleChildEval<T : QueryEvalNode>(val child: T) : QueryEvalNode {
}

class DirichletSmoothingEval(val child: CountEvalNode, val mu: Double) : QueryEvalNode {
    val background = mu * child.getCountStats().countProbability()
    override fun score(doc: Int): Float {
        val c = child.count(doc).toDouble()
        val length = child.length(doc).toDouble()
        return Math.log((c+ background) / (length + mu)).toFloat()
    }

    override fun matches(doc: Int): Boolean = child.matches(doc)
    override fun estimateDF(): Long = child.estimateDF()

    override fun count(doc: Int): Int = TODO("not yet")
    override fun explain(doc: Int): Explanation = TODO("not yet")
}