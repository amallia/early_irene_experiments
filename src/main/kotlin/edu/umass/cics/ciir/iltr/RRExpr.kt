package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.sprf.GExpr
import edu.umass.cics.ciir.sprf.cfProbability
import org.lemurproject.galago.core.retrieval.LocalRetrieval

/**
 * @author jfoley
 */
class RREnv(val retr: LocalRetrieval) {
    val mu = 1500.0
    val lengths = retr.getCollectionStatistics(GExpr("lengths"))

    fun mean(exprs: List<RRExpr>) = RRMean(this, exprs)
    fun mean(vararg exprs: RRExpr) = RRMean(this, exprs.toList())
    fun sum(exprs: List<RRExpr>) = RRSum(this, exprs)
    fun sum(vararg exprs: RRExpr) = RRSum(this, exprs.toList())
    fun term(term: String) = RRDirichletTerm(this, term)
    fun feature(name: String) = RRFeature(this, name)
}

sealed class RRExpr(val env: RREnv) {
    abstract fun eval(doc: LTRDoc): Double
    fun weighted(weight: Double) = RRWeighted(env, weight, this)
    fun checkNaNs() = RRNaNCheck(env, this)
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
        return score;
    }
}
class RRWeighted(env: RREnv, val weight: Double, inner: RRExpr): RRSingleChildExpr(env, inner) {
    override fun eval(doc: LTRDoc): Double {
        return weight * inner.eval(doc)
    }
}

sealed class RRCCExpr(env: RREnv, val exprs: List<RRExpr>): RRExpr(env)
class RRSum(env: RREnv, exprs: List<RRExpr>): RRCCExpr(env, exprs) {
    override fun eval(doc: LTRDoc): Double {
        var sum = 0.0;
        exprs.forEach {
            sum += it.eval(doc)
        }
        return sum
    }
}
class RRMean(env: RREnv, exprs: List<RRExpr>): RRCCExpr(env, exprs) {
    val N = exprs.size.toDouble()
    override fun eval(doc: LTRDoc): Double {
        var sum = 0.0;
        exprs.forEach {
            sum += it.eval(doc)
        }
        return sum / N
    }
}

sealed class RRLeafExpr(env: RREnv) : RRExpr(env)
class RRDirichletTerm(env: RREnv, val term: String, val mu: Double = env.mu) : RRLeafExpr(env) {
    val tStats = env.retr.getNodeStatistics(GExpr("counts", term))
    val bgStats = env.lengths
    val bg = mu * tStats.cfProbability(bgStats)

    override fun eval(doc: LTRDoc): Double {
        val count = doc.freqs.count(term).toDouble()
        val length = doc.freqs.length + mu
        return Math.log((count + bg) / length)
    }
}
class RRFeature(env: RREnv, val name: String): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        return doc.features[name]!!
    }
}

