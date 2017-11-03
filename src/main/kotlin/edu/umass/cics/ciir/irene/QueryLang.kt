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
}

sealed class QExpr() {
    abstract val children: List<QExpr>
}
sealed class LeafExpr() : QExpr() {
    override val children: List<QExpr> get() = emptyList()
}
sealed class OpExpr(override val children: List<QExpr>) : QExpr()
sealed class SingleChildExpr(val child: QExpr) : QExpr() {
    override val children: List<QExpr> get() = listOf(child)
}
class RequireExpr(var cond: QExpr, var value: QExpr): QExpr() {
    override val children: List<QExpr> get() = arrayListOf(cond, value)
}
class TextExpr(var text: String, var field: String? = null) : LeafExpr() {
    constructor(term: Term) : this(term.text(), term.field())
}
class SynonymExpr(children: List<QExpr>): OpExpr(children)
class LuceneExpr(val rawQuery: String) : LeafExpr() {
    var query: LuceneQuery? = null
    fun parse(env: IreneQueryLanguage) {
        query = lucene_try {
            env.luceneQueryParser.parse(rawQuery)
        } ?: error("Could not parse lucene expression: ``${rawQuery}''")
    }
}

class AndExpr(children: List<QExpr>) : OpExpr(children)
class OrExpr(children: List<QExpr>) : OpExpr(children)

class SumExpr(children: List<QExpr>) : OpExpr(children)
class MeanExpr(children: List<QExpr>) : OpExpr(children)
class MultExpr(children: List<QExpr>) : OpExpr(children)
class MaxExpr(children: List<QExpr>) : OpExpr(children)
class WeightExpr(child: QExpr, val weight: Double = 1.0) : SingleChildExpr(child)

class DirQLExpr(child: QExpr, var mu: Double? = null): SingleChildExpr(child)
class BM25Expr(child: QExpr, var b: Double? = null, var k: Double? = null): SingleChildExpr(child)
class CountToScoreExpr(child: QExpr): SingleChildExpr(child)
class BoolToScoreExpr(child: QExpr, var trueScore: Double=1.0, var falseScore: Double=0.0): SingleChildExpr(child)
class CountToBoolExpr(child: QExpr, var gt: Int = 0): SingleChildExpr(child)

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
    is OpExpr -> pmake {
        set("op", when(q) {
            is SynonymExpr -> "syn"
            is AndExpr -> "band"
            is OrExpr -> "bor"
            is SumExpr -> "combine"
            is MeanExpr -> "mean"
            is MultExpr -> "wsum"
            is MaxExpr -> "max"
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
    is SingleChildExpr -> pmake {
        set("op", "cast")
        set("kind", when(q) {
            is CountToScoreExpr -> "count->score"
            is BoolToScoreExpr -> "bool->score"
            is CountToBoolExpr -> "count->bool"
            else -> TODO(q.javaClass.canonicalName)
        })
        set("child", toJSON(q.child))
    }
}

fun applyEnvironment(env: IreneQueryLanguage, q: QExpr) {
    when(q) {
        is TextExpr -> if(q.field == null) {
            q.field = env.defaultField
        } else {}
        is LuceneExpr -> q.parse(env)
        is DirQLExpr -> if (q.mu == null) {
            q.mu = env.defaultDirichletMu
        }
        is BM25Expr -> {
            if (q.b == null) q.b = env.defaultBM25b
            if (q.k == null) q.k = env.defaultBM25k
        }
    }
    q.children.forEach { applyEnvironment(env, it) }
}

fun main(args: Array<String>) {
    val complicated = RequireExpr(
            CountToBoolExpr(TextExpr("funds")),
            SumExpr(listOf(
                    DirQLExpr(TextExpr("president")),
                    DirQLExpr(TextExpr("query"))
            )))
    println(toJSON(complicated).toPrettyString())
    applyEnvironment(IreneQueryLanguage(), complicated)
    println(toJSON(complicated).toPrettyString())
}

