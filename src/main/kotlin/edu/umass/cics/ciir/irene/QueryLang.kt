package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.sprf.pmake
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import org.lemurproject.galago.utility.Parameters

const val DEFAULT_KEY: String = "default";
/**
 *
 * @author jfoley.
 */
class IreneQueryLanguage(val analyzer: Analyzer = WhitespaceAnalyzer()) {
    var defaultField = "body"
    var defaultScorer = "dirichlet"
    var luceneQueryParser = QueryParser(defaultField, analyzer)
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
class LuceneExpr(val rawQuery: String) : LeafExpr()

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

fun main(args: Array<String>) {
    val complicated = RequireExpr(
            CountToBoolExpr(TextExpr("funds")),
            SumExpr(listOf(
                    DirQLExpr(TextExpr("president")),
                    DirQLExpr(TextExpr("query"))
            )))
    println(toJSON(complicated).toPrettyString())
}


/*
class QExpr(val operator: String) {
    companion object {
        fun make(operator: String, setup: QExpr.() -> Unit): QExpr {
            return QExpr(operator).apply(setup)
        }
        fun term(t: Term) = make("text") {
            default = t.text()
            config.set("field", t.field())
        }
    }
    val isLeaf: Boolean
        get() = children.size == 0
    val config = Parameters.create()
    val children = ArrayList<QExpr>()

    // Common but optional query parameters.
    var default: String?
        get() = config.get(DEFAULT_KEY, null as String?)
        set(value) = config.set(DEFAULT_KEY, value)
    var field: String?
        get() = config.get("field", null as String?)
        set(value) = config.set("field", value)
    var weight: Double
        get() = config.get("weight", 1.0)
        set(value) = config.set("weight", value)

    override fun equals(other: Any?): Boolean {
        if (other is QExpr) {
            if (other.children.size != children.size) return false
            if (other.operator != operator) return false
            if (other.config != config) return false
            if (other.children != children) return false
            return true
        }
        return false
    }

    override fun hashCode(): Int {
        return operator.hashCode() xor children.hashCode()
    }

    fun copy(): QExpr {
        return copy(children.map { it.copy() })
    }
    fun copy(newChildren: List<QExpr>): QExpr {
        val clone = QExpr(operator)
        clone.config.copyFrom(config)
        clone.children.addAll(newChildren)
        return clone
    }

    fun hasDefault() = config.containsKey(DEFAULT_KEY)

    fun addChild(c: QExpr) { children.add(c) }
    fun toJSON(): Parameters = pmake {
        set("operator", operator)
        set("config", config)
        set("children", children.map { it.toJSON() })
    }

}

interface Macro {
    fun transform(input: QExpr): QExpr?
    fun transform(input: List<QExpr>) = input.map { transform(it) }.filterNotNull()
}
class InsertScorersMacro(val ql: IreneQueryLanguage) : Macro {
    override fun transform(input: QExpr): QExpr? {
    }
}
*/
