package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.sprf.pmake
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import org.lemurproject.galago.utility.Parameters

typealias LuceneQuery = org.apache.lucene.search.Query

/**
 *
 * @author jfoley.
 */
class IreneQueryLanguage(val analyzer: Analyzer = WhitespaceAnalyzer()) {
    var defaultField = "body"
    var defaultScorer = "dirichlet"
    var luceneQueryParser = QueryParser(defaultField, analyzer)
    var defaultDirichletMu: Double = 1500.0
    var defaultBM25b: Double = 0.75
    var defaultBM25k: Double = 1.2

    fun simplify(q: QExpr): QExpr {
        val pq = q.copy()

        // combine weights until query stops changing.
        while(combineWeights(pq)) { }

        return pq
    }

    fun prepare(index: IreneIndex, q: QExpr): QExpr {
        val pq = simplify(q)
        applyEnvironment(this, pq)
        analyzeDataNeededRecursive(q)
        applyIndex(index, pq)
        return pq
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as IreneQueryLanguage

        if (defaultField != other.defaultField) return false
        if (defaultScorer != other.defaultScorer) return false
        if (defaultDirichletMu != other.defaultDirichletMu) return false
        if (defaultBM25b != other.defaultBM25b) return false
        if (defaultBM25k != other.defaultBM25k) return false

        return true
    }

    override fun hashCode(): Int {
        var result = defaultField.hashCode()
        result = 31 * result + defaultScorer.hashCode()
        result = 31 * result + defaultDirichletMu.hashCode()
        result = 31 * result + defaultBM25b.hashCode()
        result = 31 * result + defaultBM25k.hashCode()
        return result
    }

}

// Easy "model"-based constructor.
fun QueryLikelihood(terms: List<String>, field: String?=null, mu: Double?=null): QExpr {
    return MeanExpr(terms.map { DirQLExpr(TextExpr(it, field), mu) })
}

sealed class QExpr {
    abstract val children: List<QExpr>
    abstract fun copy(): QExpr
    fun copyChildren() = children.map { it.copy() }

    fun visit(each: (QExpr)->Unit) {
        each(this)
        children.forEach { it.visit(each) }
    }

    fun weighted(x: Double) = WeightExpr(this, x)
}
sealed class LeafExpr : QExpr() {
    override val children: List<QExpr> get() = emptyList()
}
sealed class OpExpr : QExpr() {
}
sealed class SingleChildExpr : QExpr() {
    abstract var child: QExpr
    override val children: List<QExpr> get() = listOf(child)
}
data class RequireExpr(var cond: QExpr, var value: QExpr): QExpr() {
    override fun copy()  = RequireExpr(cond.copy(), value.copy())
    override val children: List<QExpr> get() = arrayListOf(cond, value)
}
data class TextExpr(var text: String, var field: String? = null, var stats: CountStats? = null, var needed: DataNeeded = DataNeeded.DOCS) : LeafExpr() {
    override fun copy() = TextExpr(text, field, stats, needed)
    constructor(term: Term) : this(term.text(), term.field())
}
data class SynonymExpr(override val children: List<QExpr>): OpExpr() {
    override fun copy() = SynonymExpr(copyChildren())
}
data class LuceneExpr(val rawQuery: String, var query: LuceneQuery? = null ) : LeafExpr() {
    fun parse(env: IreneQueryLanguage) = LuceneExpr(rawQuery,
        lucene_try {
            env.luceneQueryParser.parse(rawQuery)
        } ?: error("Could not parse lucene expression: ``${rawQuery}''"))
    override fun copy() = LuceneExpr(rawQuery, query)
}

data class AndExpr(override val children: List<QExpr>) : OpExpr() {
    override fun copy() = AndExpr(copyChildren())
}
data class OrExpr(override val children: List<QExpr>) : OpExpr() {
    override fun copy() = OrExpr(copyChildren())
}
fun SumExpr(vararg children: QExpr) = SumExpr(children.toList())
fun SumExpr(children: List<QExpr>) = CombineExpr(children, children.map { 1.0 })
fun MeanExpr(vararg children: QExpr) = MeanExpr(children.toList())
fun MeanExpr(children: List<QExpr>) = CombineExpr(children, children.map { 1.0 / children.size.toDouble() })
data class CombineExpr(override var children: List<QExpr>, var weights: List<Double>) : OpExpr() {
    override fun copy() = CombineExpr(copyChildren(), weights)
}
data class MultExpr(override val children: List<QExpr>) : OpExpr() {
    override fun copy() = MultExpr(copyChildren())
}
data class MaxExpr(override val children: List<QExpr>) : OpExpr() {
    override fun copy() = MaxExpr(copyChildren())
}

data class OrderedWindowExpr(override var children: List<QExpr>, var step: Int=1) : OpExpr() {
    var computedStats: CountStats? = null
    override fun copy() = OrderedWindowExpr(copyChildren(), step)

}
data class WeightExpr(override var child: QExpr, var weight: Double = 1.0) : SingleChildExpr() {
    override fun copy() = WeightExpr(child.copy(), weight)
}

data class DirQLExpr(override var child: QExpr, var mu: Double? = null): SingleChildExpr() {
    override fun copy() = DirQLExpr(child.copy(), mu)
}
data class BM25Expr(override var child: QExpr, var b: Double? = null, var k: Double? = null): SingleChildExpr() {
    override fun copy() = BM25Expr(child.copy(), b, k)
}
data class CountToScoreExpr(override var child: QExpr): SingleChildExpr() {
    override fun copy() = CountToScoreExpr(child.copy())
}
data class BoolToScoreExpr(override var child: QExpr, var trueScore: Double=1.0, var falseScore: Double=0.0): SingleChildExpr() {
    override fun copy() = BoolToScoreExpr(child.copy(), trueScore, falseScore)
}
data class CountToBoolExpr(override var child: QExpr, var gt: Int = 0): SingleChildExpr() {
    override fun copy() = CountToBoolExpr(child.copy(), gt)
}

fun toJSON(q: QExpr): Parameters = when(q) {
    is TextExpr -> pmake {
        set("op", "text")
        set("text", q.text)
        putIfNotNull("field", q.field)
    }
    is LuceneExpr -> pmake {
        set("op", "lucene")
        set("rawQuery", q.rawQuery)
    }
    is RequireExpr -> pmake {
        set("op", "require")
        set("cond", toJSON(q.cond))
        set("value", toJSON(q.value))
    }
    is CombineExpr -> pmake {
        set("op", "combine")
        set("weights", q.weights)
        set("children", q.children.map { toJSON(it) })
    }
    is OpExpr -> pmake {
        set("op", when(q) {
            is SynonymExpr -> "syn"
            is AndExpr -> "band"
            is OrExpr -> "bor"
            is MultExpr -> "wsum"
            is MaxExpr -> "max"
            else -> error("Handle this elsewhere!")
        })
        set("children", q.children.map { toJSON(it) })
    }
    is WeightExpr -> pmake {
        set("op", "weight")
        set("w", q.weight)
        set("child", toJSON(q.child))
    }
    is DirQLExpr -> pmake {
        set("op", "dirichlet")
        putIfNotNull("mu", q.mu)
        set("child", toJSON(q.child))
    }
    is BM25Expr -> pmake {
        set("op", "bm25")
        putIfNotNull("b", q.b)
        putIfNotNull("k", q.k)
        set("child", toJSON(q.child))
    }
    is CountToScoreExpr -> pmake {
        set("op", "count->score")
        set("child", toJSON(q.child))
    }
    is BoolToScoreExpr -> pmake {
        set("op", "bool->score")
        set("child", toJSON(q.child))
        set("true", q.trueScore)
        set("false", q.falseScore)
    }
    is CountToBoolExpr -> pmake {
        set("op", "count->bool")
        set("child", toJSON(q.child))
        set("gt", q.gt)
    }
}

fun combineWeights(q: QExpr): Boolean {
    var changed = false
    when(q) {
        // Flatten nested-weights.
        is WeightExpr -> {
            val c = q.child
            if (c is WeightExpr) {
                q.child = c.child
                q.weight *= c.weight
                changed = true
                //println("weight(weight(x)) -> weight(x)")
            }
        }
        // Pull weights up into CombineExpr.
        is CombineExpr -> {
            val newChildren = arrayListOf<QExpr>()
            val newWeights = arrayListOf<Double>()
            q.children.zip(q.weights).forEach { (c, w) ->
                if (c is CombineExpr) {
                    // flatten combine(combine(...))
                    c.children.zip(c.weights).forEach { (cc, cw) ->
                        newChildren.add(cc)
                        newWeights.add(w*cw)
                    }
                    changed = true
                } else if (c is WeightExpr) {
                    //println("combine(...weight(x)..) -> combine(...x...)")
                    newChildren.add(c.child)
                    newWeights.add(c.weight * w)
                    changed = true
                } else {
                    newChildren.add(c)
                    newWeights.add(w)
                }
                q.children = newChildren
                q.weights = newWeights
            }
        }
        else -> {}
    }
    q.children.forEach {
        changed = changed || combineWeights(it)
    }
    return changed
}

class TypeCheckError(msg: String): Exception(msg)

fun analyzeDataNeededRecursive(q: QExpr, needed: DataNeeded=DataNeeded.DOCS) {
    var childNeeds = needed
    childNeeds = when(q) {
        is TextExpr -> {
            if (childNeeds == DataNeeded.SCORES) {
                throw TypeCheckError("Cannot convert q=$q to score. Try DirQLExpr(q) or BM25Expr(q)")
            }
            q.needed = childNeeds
            childNeeds
        }
        is AndExpr, is OrExpr -> DataNeeded.DOCS
        is LuceneExpr, is SynonymExpr -> childNeeds
        is WeightExpr, is CombineExpr, is MultExpr, is MaxExpr -> {
            DataNeeded.SCORES
        }
        is OrderedWindowExpr -> {
            if (q.children.size <= 1) {
                throw TypeCheckError("Need more than 1 child for an OrderedWindow, e.g. $q")
            }
            DataNeeded.POSITIONS
        }
        is BM25Expr, is DirQLExpr ->  DataNeeded.COUNTS
        is CountToScoreExpr ->  DataNeeded.COUNTS
        is BoolToScoreExpr -> DataNeeded.DOCS
        is CountToBoolExpr -> DataNeeded.COUNTS
        is RequireExpr -> {
            analyzeDataNeededRecursive(q.cond, DataNeeded.DOCS)
            analyzeDataNeededRecursive(q.value, childNeeds)
            return
        }
    }
    q.children.forEach { analyzeDataNeededRecursive(it, childNeeds) }
}

fun applyEnvironment(env: IreneQueryLanguage, root: QExpr) {
    root.visit { q ->
        when(q) {
            is TextExpr -> if(q.field == null) {
                q.field = env.defaultField
            } else { }
            is LuceneExpr -> q.parse(env)
            is DirQLExpr -> if (q.mu == null) {
                q.mu = env.defaultDirichletMu
            }
            is BM25Expr -> {
                if (q.b == null) q.b = env.defaultBM25b
                if (q.k == null) q.k = env.defaultBM25k
            }
            else -> {}
        }
    }
}

fun applyIndex(index: IreneIndex, root: QExpr) {
    root.visit { q ->
        if (q is TextExpr) {
            val field = q.field ?: error("Need a field for $q")
            q.stats = index.getStats(Term(field, q.text))
            // Warning, q is missing.
            if (q.stats == null) {
                error("Query uses field ``$field'' which does not exist in index, via $q")
            }
        }
    }
}

fun main(args: Array<String>) {
    val complicated = RequireExpr(
            CountToBoolExpr(TextExpr("funds")),
            SumExpr(listOf(
                    DirQLExpr(TextExpr("president")),
                    DirQLExpr(TextExpr("query"))
            )))
    println(complicated)
    println(toJSON(complicated).toPrettyString())
    applyEnvironment(IreneQueryLanguage(), complicated)
    println(toJSON(complicated).toPrettyString())
    println(complicated)
}

