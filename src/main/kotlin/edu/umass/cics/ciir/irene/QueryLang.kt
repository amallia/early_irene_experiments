package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.chai.forAllPairs
import edu.umass.cics.ciir.chai.forEachSeqPair
import edu.umass.cics.ciir.iltr.RREnv
import org.apache.lucene.index.Term
import org.apache.lucene.queryparser.classic.QueryParser
import java.util.*

typealias LuceneQuery = org.apache.lucene.search.Query

/**
 *
 * @author jfoley.
 */
class IreneQueryLanguage(val index: IIndex = EmptyIndex()) : RREnv() {
    override fun fieldStats(field: String): CountStats = index.fieldStats(field) ?: error("Requested field $field does not exist.")
    override fun computeStats(q: QExpr): CountStats = index.getStats(q)
    override fun getStats(term: String, field: String?): CountStats = index.getStats(term, field ?: defaultField)
    val luceneQueryParser: QueryParser
        get() = QueryParser(defaultField, (index as IreneIndex).analyzer)
    override var estimateStats: String? = null
}

fun simplify(q: QExpr): QExpr {
    val pq = q.copy()
    // combine weights until query stops changing.
    while(combineWeights(pq)) { }
    return pq
}

fun SmartStop(terms: List<String>, stopwords: Set<String>): List<String> {
    val nonStop = terms.filter { !stopwords.contains(it) }
    if (nonStop.isEmpty()) {
        return terms
    }
    return nonStop
}

// Easy "model"-based constructor.
fun QueryLikelihood(terms: List<String>, field: String?=null, statsField: String?=null, mu: Double? = null): QExpr {
    return UnigramRetrievalModel(terms, {DirQLExpr(it, mu)}, field, statsField)
}
fun BM25Model(terms: List<String>, field: String?=null, statsField: String?=null, b: Double? = null, k: Double? = null): QExpr {
    return UnigramRetrievalModel(terms, { BM25Expr(it, b, k) }, field, statsField)
}
fun UnigramRetrievalModel(terms: List<String>, scorer: (TextExpr)->QExpr, field: String?=null, statsField: String?=null): QExpr {
    return MeanExpr(terms.map { scorer(TextExpr(it, field, statsField)) })
}

fun SequentialDependenceModel(terms: List<String>, field: String?=null, statsField: String?=null, stopwords: Set<String> =emptySet(), uniW: Double = 0.8, odW: Double = 0.15, uwW: Double = 0.05, odStep: Int=1, uwWidth:Int=8, fullProx: Double? = null, fullProxWidth:Int=12, makeScorer: (QExpr)->QExpr = {DirQLExpr(it)}): QExpr {
    if (terms.isEmpty()) throw IllegalStateException("Empty SDM")
    if (terms.size == 1) {
        return makeScorer(TextExpr(terms[0], field, statsField))
    }

    val nonStop = terms.filterNot { stopwords.contains(it) }
    val bestTerms = (if (nonStop.isNotEmpty()) { nonStop } else terms)
    val unigrams: List<QExpr> = bestTerms
            .map { makeScorer(TextExpr(it, field, statsField)) }

    val bigrams = ArrayList<QExpr>()
    val ubigrams = ArrayList<QExpr>()
    terms.forEachSeqPair { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        bigrams.add(makeScorer(OrderedWindowExpr(ts, odStep)))
    }
    bestTerms.forEachSeqPair { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        ubigrams.add(makeScorer(UnorderedWindowExpr(ts, uwWidth)))
    }

    val exprs = arrayListOf(
            MeanExpr(unigrams).weighted(uniW),
            MeanExpr(bigrams).weighted(odW),
            MeanExpr(ubigrams).weighted(uwW))

    if (fullProx != null) {
        val fullProxTerms = if (bestTerms.size >= 2) bestTerms else terms
        exprs.add(makeScorer(UnorderedWindowExpr(fullProxTerms.map { TextExpr(it, field, statsField) }, fullProxWidth)).weighted(fullProx))
    }

    return SumExpr(exprs)
}

fun FullDependenceModel(terms: List<String>, field: String?=null, statsField: String? = null, stopwords: Set<String> =emptySet(), uniW: Double = 0.8, odW: Double = 0.15, uwW: Double = 0.05, odStep: Int=1, uwWidth:Int=8, fullProx: Double? = null, fullProxWidth:Int=12, makeScorer: (QExpr)->QExpr = {DirQLExpr(it)}): QExpr {
    if (terms.isEmpty()) throw IllegalStateException("Empty FDM")
    if (terms.size == 1) {
        return makeScorer(TextExpr(terms[0], field, statsField))
    }

    val nonStop = terms.filterNot { stopwords.contains(it) }
    val bestTerms = (if (nonStop.isNotEmpty()) { nonStop } else terms)
    val unigrams: List<QExpr> = bestTerms
            .map { makeScorer(TextExpr(it, field, statsField)) }

    val bigrams = ArrayList<QExpr>()
    val ubigrams = ArrayList<QExpr>()
    terms.forAllPairs { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        bigrams.add(makeScorer(OrderedWindowExpr(ts, odStep)))
    }
    bestTerms.forAllPairs { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        ubigrams.add(makeScorer(UnorderedWindowExpr(ts, uwWidth)))
    }

    val exprs = arrayListOf(
            MeanExpr(unigrams).weighted(uniW),
            MeanExpr(bigrams).weighted(odW),
            MeanExpr(ubigrams).weighted(uwW))

    if (fullProx != null) {
        val fullProxTerms = if (bestTerms.size >= 2) bestTerms else terms
        exprs.add(makeScorer(UnorderedWindowExpr(fullProxTerms.map { TextExpr(it, field, statsField) }, fullProxWidth)).weighted(fullProx))
    }

    return SumExpr(exprs)
}


sealed class QExpr {
    abstract val children: List<QExpr>
    abstract fun copy(): QExpr
    fun copyChildren() = children.map { it.copy() }

    fun visit(each: (QExpr)->Unit) {
        each(this)
        children.forEach { it.visit(each) }
    }

    fun getStatsFields(): Set<String> {
        val out = HashSet<String>()
        visit { c ->
            if (c is TextExpr) {
                out.add(c.statsField())
            }
        }
        return out
    }
    fun getSingleStatsField(default: String): String {
        val fields = getStatsFields()
        return when(fields.size) {
            0 -> default
            1 -> fields.first()
            else -> error("Can't determine single field for $this")
        }
    }

    // Get a weighted version of this node if weight is non-null.
    fun weighted(x: Double?) = if(x != null) WeightExpr(this, x) else this
}
data class MultiExpr(val namedExprs: Map<String, QExpr>): QExpr() {
    val names = namedExprs.keys.toList()
    override val children = names.map { namedExprs[it]!! }.toList()
    override fun copy() = MultiExpr(namedExprs.mapValues { (_, v) -> v.copy() })
}
sealed class LeafExpr : QExpr() {
    override val children: List<QExpr> get() = emptyList()
}
sealed class ConstExpr : LeafExpr()
data class ConstScoreExpr(var x: Double): ConstExpr() {
    override fun copy(): QExpr = ConstScoreExpr(x)
}
data class ConstCountExpr(var x: Int, val lengths: LengthsExpr): ConstExpr() {
    override fun copy(): QExpr = ConstCountExpr(x, lengths)
}
data class ConstBoolExpr(var x: Boolean): ConstExpr() {
    override fun copy(): QExpr = ConstBoolExpr(x)
}

data class LengthsExpr(var statsField: String?, var stats: CountStats? = null) : LeafExpr() {
    fun applyEnvironment(env: RREnv) {
        if (statsField == null) {
            statsField = env.defaultField
        }
    }
    override fun copy() = LengthsExpr(statsField, stats)
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
data class TextExpr(var text: String, private var field: String? = null, private var statsField: String? = null, var stats: CountStats? = null, var needed: DataNeeded = DataNeeded.DOCS) : LeafExpr() {
    override fun copy() = TextExpr(text, field, statsField, stats, needed)
    constructor(term: Term) : this(term.text(), term.field())

    fun applyEnvironment(env: RREnv) {
        if (field == null) {
            field = env.defaultField
        }
        if (statsField == null) {
            statsField = env.defaultField
        }
    }
    fun countsField(): String = field ?: error("No primary field for $this")
    fun statsField(): String = statsField ?: field ?: error("No stats field for $this")
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

data class MinCountExpr(override var children: List<QExpr>): OpExpr() {
    override fun copy() = MinCountExpr(copyChildren())
}
data class OrderedWindowExpr(override var children: List<QExpr>, var step: Int=1) : OpExpr() {
    override fun copy() = OrderedWindowExpr(copyChildren(), step)
}
data class UnorderedWindowExpr(override var children: List<QExpr>, var width: Int=4) : OpExpr() {
    override fun copy() = UnorderedWindowExpr(copyChildren(), width)
}

data class WeightExpr(override var child: QExpr, var weight: Double = 1.0) : SingleChildExpr() {
    override fun copy() = WeightExpr(child.copy(), weight)
}

data class DirQLExpr(override var child: QExpr, var mu: Double? = null): SingleChildExpr() {
    override fun copy() = DirQLExpr(child.copy(), mu)
}
data class AbsoluteDiscountingQLExpr(override var child: QExpr, var delta: Double? = null): SingleChildExpr() {
    override fun copy() = AbsoluteDiscountingQLExpr(child.copy(), delta)
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
        is LengthsExpr -> return
        is AndExpr, is OrExpr -> DataNeeded.DOCS
        // Pass through whatever at this point.
        is MultiExpr -> childNeeds
        is LuceneExpr, is SynonymExpr -> childNeeds
        is WeightExpr, is CombineExpr, is MultExpr, is MaxExpr -> {
            DataNeeded.SCORES
        }
        is MinCountExpr -> {
            if (q.children.size <= 1) {
                throw TypeCheckError("Need more than 1 child for an count summary Expr, e.g. $q")
            }
            DataNeeded.COUNTS
        }
        is UnorderedWindowExpr, is OrderedWindowExpr -> {
            if (q.children.size <= 1) {
                throw TypeCheckError("Need more than 1 child for an window Expr, e.g. $q")
            }
            DataNeeded.POSITIONS
        }
        is AbsoluteDiscountingQLExpr, is BM25Expr, is DirQLExpr ->  DataNeeded.COUNTS
        is CountToScoreExpr ->  DataNeeded.COUNTS
        is BoolToScoreExpr -> DataNeeded.DOCS
        is CountToBoolExpr -> DataNeeded.COUNTS
        is RequireExpr -> {
            analyzeDataNeededRecursive(q.cond, DataNeeded.DOCS)
            analyzeDataNeededRecursive(q.value, childNeeds)
            return
        }
        is ConstScoreExpr -> return assert(needed == DataNeeded.SCORES)
        is ConstCountExpr -> return assert(needed == DataNeeded.COUNTS)
        is ConstBoolExpr -> return assert(needed == DataNeeded.DOCS)
    }
    q.children.forEach { analyzeDataNeededRecursive(it, childNeeds) }
}

fun applyEnvironment(env: RREnv, root: QExpr) {
    root.visit { q ->
        when(q) {
            is LengthsExpr -> q.applyEnvironment(env)
            is TextExpr -> q.applyEnvironment(env)
            is LuceneExpr -> q.parse(env as? IreneQueryLanguage ?: error("LuceneExpr in environment without LuceneParser."))
            is DirQLExpr -> if (q.mu == null) {
                q.mu = env.defaultDirichletMu
            }
            is AbsoluteDiscountingQLExpr -> if (q.delta == null) {
                q.delta = env.absoluteDiscountingDelta
            }
            is BM25Expr -> {
                if (q.b == null) q.b = env.defaultBM25b
                if (q.k == null) q.k = env.defaultBM25k
            }
            else -> {}
        }
    }
}

fun computeQExprStats(index: RREnv, root: QExpr) {
    root.visit { q ->
        when (q) {
            is TextExpr -> {
                q.stats = index.getStats(q.text, q.statsField())
            }
            is LengthsExpr -> {
                q.stats = index.fieldStats(q.statsField!!)
            }
            else -> { }
        }
    }
}

