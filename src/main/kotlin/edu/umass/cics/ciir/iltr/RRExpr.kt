package edu.umass.cics.ciir.iltr

import com.github.benmanes.caffeine.cache.Caffeine
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.irene.scoring.PositionsIter
import edu.umass.cics.ciir.sprf.GExpr
import edu.umass.cics.ciir.sprf.setf
import org.lemurproject.galago.core.index.stats.FieldStatistics
import org.lemurproject.galago.core.retrieval.LocalRetrieval

/**
 * @author jfoley
 */
class RREnv(val retr: LocalRetrieval) {
    var defaultField = "document"
    var mu = 1500.0
    var bm25b = 0.75
    var bm25k = 1.2
    var absoluteDiscountingDelta = 0.7
    val lengths = retr.getCollectionStatistics(GExpr("lengths"))!!
    val lengthsInfo = HashMap<String, FieldStatistics>()
    private fun getFieldStats(field: String): FieldStatistics {
        return lengthsInfo.computeIfAbsent(field, {retr.getCollectionStatistics(GExpr("lengths", field))})
    }

    fun getStats(term: String, field: String?=null): CountStats {
        val fieldName = field ?: defaultField
        val t = TextExpr(term, fieldName)
        val node = GExpr("counts", t.text).apply { setf("field", fieldName) }
        val stats = retr.getNodeStatistics(node)
        val fstats = getFieldStats(fieldName)

        return CountStats(t.toString(),
                cf=stats.nodeFrequency,
                df=stats.nodeDocumentCount,
                dc=fstats.documentCount,
                cl=fstats.collectionLength)
    }

    fun mean(exprs: List<RRExpr>) = RRMean(this, exprs)
    fun mean(vararg exprs: RRExpr) = RRMean(this, exprs.toList())
    fun sum(exprs: List<RRExpr>) = RRSum(this, exprs)
    fun sum(vararg exprs: RRExpr) = RRSum(this, exprs.toList())
    fun term(term: String) = RRDirichletTerm(this, term)
    fun feature(name: String) = RRFeature(this, name)

    fun ql(terms: List<String>) = mean(terms.map { RRDirichletTerm(this, it) })
    fun bm25(terms: List<String>) = sum(terms.map { RRBM25Term(this, it) })
    fun mult(vararg exprs: RRExpr) = RRMult(this, exprs.toList())
    fun const(x: Double) = RRConst(this, x)

    fun fromQExpr(q: QExpr): RRExpr = when(q) {
        is TextExpr -> RRTermExpr(this, q.text, q.field!!)
        is LuceneExpr -> error("Can't support LuceneExpr.")
        is SynonymExpr -> TODO()
        is AndExpr -> TODO()
        is OrExpr -> TODO()
        // make sum of weighted:
        is CombineExpr -> sum(q.children.zip(q.weights).map { (child, weight) ->
            fromQExpr(child).weighted(weight)
        })
        is MultExpr -> RRMult(this, q.children.map { fromQExpr(it) })
        is MaxExpr -> RRMax(this, q.children.map { fromQExpr(it) })
        is OrderedWindowExpr -> TODO()
        is UnorderedWindowExpr -> TODO()
        is WeightExpr -> RRWeighted(this, q.weight, fromQExpr(q.child))
        is DirQLExpr -> RRDirichletScorer(this, fromQExpr(q.child) as RRCountExpr, q.mu ?: mu)
        is BM25Expr -> RRBM25Scorer(this, fromQExpr(q.child) as RRCountExpr, q.b ?: bm25b, q.k ?: bm25k)
        is CountToScoreExpr -> TODO()
        is BoolToScoreExpr -> TODO()
        is CountToBoolExpr -> TODO()
        is RequireExpr -> TODO()
        is ConstScoreExpr -> RRConst(this, q.x)
        is ConstCountExpr -> RRConst(this, q.x.toDouble())
        is ConstBoolExpr -> RRConst(this, if (q.x) 1.0 else 0.0)
    }
}

fun QExpr.toRRExpr(env: RREnv): RRExpr {
    val q = simplify(this)
    analyzeDataNeededRecursive(q)
    return env.fromQExpr(q)
}

sealed class RRExpr(val env: RREnv) {
    abstract fun eval(doc: LTRDoc): Double
    fun weighted(weight: Double) = RRWeighted(env, weight, this)
    fun checkNaNs() = RRNaNCheck(env, this)

    // Take ((weight * this) + (1-weight) * e)
    fun mixed(weight: Double, e: RRExpr) = RRSum(env, arrayListOf(this.weighted(weight), e.weighted(1.0 - weight)))
}
sealed class RRSingleChildExpr(env: RREnv, val inner: RRExpr): RRExpr(env)
class RRNaNCheck(env: RREnv, inner: RRExpr): RRSingleChildExpr(env, inner) {
    override fun eval(doc: LTRDoc): Double {
        val score = inner.eval(doc)
        if (java.lang.Double.isNaN(score)) {
            throw RuntimeException("NaN-Check Failed.")
        }
        if (java.lang.Double.isInfinite(score)) {
            throw RuntimeException("Infinite-Check Failed.")
        }
        return score
    }
}
class RRWeighted(env: RREnv, val weight: Double, inner: RRExpr): RRSingleChildExpr(env, inner) {
    override fun eval(doc: LTRDoc): Double {
        return weight * inner.eval(doc)
    }

    override fun toString(): String = "RRWeighted($weight, $inner)"
}

sealed class RRCCExpr(env: RREnv, val exprs: List<RRExpr>): RRExpr(env) {
    init {
        assert(exprs.size > 0)
    }
}
class RRSum(env: RREnv, exprs: List<RRExpr>): RRCCExpr(env, exprs) {
    override fun eval(doc: LTRDoc): Double = exprs.sumByDouble { it.eval(doc) }
    override fun toString(): String = "RRSum($exprs)"
}
class RRMean(env: RREnv, exprs: List<RRExpr>): RRCCExpr(env, exprs) {
    val N = exprs.size.toDouble()
    override fun eval(doc: LTRDoc): Double = exprs.sumByDouble { it.eval(doc) } / N
}
class RRMax(env: RREnv, exprs: List<RRExpr>): RRCCExpr(env, exprs) {
    override fun eval(doc: LTRDoc): Double {
        return exprs.map { it.eval(doc) }.max()!!
    }
    override fun toString(): String = "RRSum($exprs)"
}

class RRMult(env: RREnv, exprs: List<RRExpr>): RRCCExpr(env, exprs) {
    override fun eval(doc: LTRDoc): Double {
        var prod = 1.0
        exprs.forEach { prod *= it.eval(doc) }
        return prod
    }
}

sealed class RRLeafExpr(env: RREnv) : RRExpr(env)
fun RRDirichletTerm(env: RREnv, term: String, field: String = env.defaultField, mu: Double = env.mu) = RRDirichletScorer(env, RRTermExpr(env, term, field), mu)
class RRDirichletScorer(env: RREnv, val term: RRCountExpr, var mu: Double = env.mu) : RRLeafExpr(env) {
    private val bg = mu * term.stats.nonzeroCountProbability()
    override fun eval(doc: LTRDoc): Double {
        val count = term.count(doc).toDouble()
        val length = term.length(doc) + mu
        return Math.log((count + bg) / length)
    }
    override fun toString(): String = "RRDirichletScorer($term)"
}
fun RRAbsoluteDiscounting(env: RREnv, term: String,  field: String = env.defaultField, delta: Double = env.absoluteDiscountingDelta) = RRAbsoluteDiscountingScorer(env, RRTermExpr(env, term, field), delta)
class RRAbsoluteDiscountingScorer(env: RREnv, val term: RRCountExpr, var delta: Double = env.absoluteDiscountingDelta) : RRLeafExpr(env) {
    val bg = term.stats.nonzeroCountProbability()
    override fun eval(doc: LTRDoc): Double {
        val field = doc.field(term.field)
        val length = field.length.toDouble()
        val uniq = field.uniqTerms.toDouble()
        val sigma = delta * (uniq / length)
        val count = maxOf(0.0, term.count(doc).toDouble() - delta)
        return Math.log((count / length) + sigma * bg)
    }
}

class RRFeature(env: RREnv, val name: String): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        return doc.features[name]!!
    }
}
class RRConst(env: RREnv, val value: Double) : RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double = value
}
data class RRTermCacheKey(val term: String, val field: String, val doc: String)

class RREnvStats(env: RREnv, val term: String, val field: String) : CountStatsStrategy() {
    val stats = env.getStats(term, field)
    override fun get(): CountStats = stats
}
sealed class RRCountExpr(env: RREnv, val field: String, val css: CountStatsStrategy) : RRExpr(env) {
    val stats: CountStats get() = css.get()
    override fun eval(doc: LTRDoc): Double = error("RRCountExpr has no inherent score.")
    abstract fun count(doc: LTRDoc) : Int
    abstract fun positions(doc: LTRDoc): PositionsIter
    fun length(doc: LTRDoc) = doc.field(field).length
    fun field(doc: LTRDoc) = doc.field(field)
}

class RRTermExpr(env: RREnv, val term: String, field: String, stats: CountStatsStrategy): RRCountExpr(env, field, stats) {
    companion object {
        val posCache = Caffeine.newBuilder().maximumSize(2000).build<RRTermCacheKey, IntArray>()
    }
    constructor(env: RREnv, term: String, field: String = env.defaultField) : this(env, term, field, RREnvStats(env, term, field))

    override fun count(doc: LTRDoc) = doc.field(field).count(term)
    override fun positions(doc: LTRDoc): PositionsIter {
        val positions = posCache.get(RRTermCacheKey(term, field, doc.name)) { (mt, mf, _) ->
            val vec = doc.field(mf).terms
            vec.mapIndexedNotNull { i, t_i ->
                if (t_i == mt) i else null
            }.toIntArray()
        } ?: intArrayOf()
        return PositionsIter(positions)
    }
}

fun RRBM25Term(env: RREnv, term: String, field: String = env.defaultField, b: Double = env.bm25b, k: Double = env.bm25k) = RRBM25Scorer(env, RRTermExpr(env, term, field), b, k)

class RRBM25Scorer(env: RREnv, val term: RRCountExpr, val b: Double = env.bm25b, val k: Double = env.bm25k) : RRLeafExpr(env) {
    private val avgDL = term.stats.avgDL();
    private val idf = Math.log(term.stats.dc / (term.stats.df + 0.5))

    override fun eval(doc: LTRDoc): Double {
        val count = term.count(doc).toDouble()
        val length = term.length(doc).toDouble()
        val num = count * (k+1.0)
        val denom = count + (k * (1.0 - b + (b * length / avgDL)))
        return idf * num / denom
    }
}

class RRAvgWordLength(env: RREnv, val field: String = env.defaultField) : RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        var sum = 0.0
        var n = 0
        doc.freqs(field).counts.forEachEntry { term, count ->
            sum += (term.length * count).toDouble()
            n += count
            true
        }
        return sum / n
    }
}

/**
 * This calculates the number of unique terms in a document divided by its length. Similar to how noisy the document is, so I called it hte DocInfoQuotient. This is used in Absolute Discounting.
 */
class RRDocInfoQuotient(env: RREnv, val field: String = env.defaultField): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        val field = doc.field(field)
        return field.uniqTerms.toDouble() / field.length
    }
}

class RRDocLength(env: RREnv, val field: String = env.defaultField) : RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double = doc.field(field).length.toDouble()
}

// Return the index of a term in a document as a fraction: for news, this should be similar to importance. A query term at the first position will receive a 1.0; halfway through the document will receive a 0.5.
class RRTermPosition(env: RREnv, val term: String, val field: String = env.defaultField): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        val field = doc.field(field)
        val length = field.length.toDouble()
        val pos = field.terms.indexOf(term).toDouble()
        if (pos < 0) {
            return 0.0
        } else {
            return 1.0 - (pos / length)
        }
    }
}

class RRJaccardSimilarity(env: RREnv, val target: Set<String>, val field: String = env.defaultField, val empty: Double=0.5): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        if (target.isEmpty()) return empty
        val field = doc.field(field)
        val uniq = field.termSet
        if (uniq.isEmpty()) return empty
        val overlap = (uniq intersect target).size.toDouble()
        val domain = (uniq union target).size.toDouble()
        return overlap / domain
    }
}

class RRLogLogisticTFScore(env: RREnv, val term: String, val field: String = env.defaultField, val c: Double = 1.0) : RRLeafExpr(env) {
    companion object {
        val OriginalPaperCValues = arrayListOf<Number>(0.5,0.75,1,2,3,4,5,6,7,8,9).map { it.toDouble() }
    }
    val stats = env.getStats(term, field)
    val avgl = stats.avgDL()
    val lambda_w = stats.binaryProbability()
    init {
        assert(stats.cf > 0) { "Term ``$term'' in field ``$field'' must actually occur in the collection for this feature to make sense." }
    }

    override fun eval(doc: LTRDoc): Double {
        val field = doc.field(field)
        val tf = field.count(term)
        val lengthRatio = avgl / field.length.toDouble()
        val t = tf * Math.log(1.0 + c * lengthRatio)
        return Math.log((t + lambda_w) / lambda_w)
    }
}