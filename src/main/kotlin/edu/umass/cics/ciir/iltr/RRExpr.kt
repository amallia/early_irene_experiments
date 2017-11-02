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
class RRNaNCheck(env: RREnv, val inner: RRExpr): RRExpr(env) {
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
class RRSum(env: RREnv, val exprs: List<RRExpr>): RRExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        return exprs.map { it.eval(doc) }.sum()
    }
}
class RRMean(env: RREnv, val exprs: List<RRExpr>): RRExpr(env) {
    val N = exprs.size.toDouble()
    override fun eval(doc: LTRDoc): Double {
        return exprs.map { it.eval(doc) }.sum() / N
    }
}
class RRWeighted(env: RREnv, val weight: Double, val e: RRExpr): RRExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        return weight * e.eval(doc)
    }
}
class RRDirichletTerm(env: RREnv, val term: String, val mu: Double = env.mu) : RRExpr(env) {
    val tStats = env.retr.getNodeStatistics(GExpr("counts", term))
    val bgStats = env.lengths
    val bg = mu * tStats.cfProbability(bgStats)

    override fun eval(doc: LTRDoc): Double {
        val count = doc.freqs.count(term).toDouble()
        val length = doc.freqs.length + mu
        return Math.log((count + bg) / length)
    }
}
class RRFeature(env: RREnv, val name: String): RRExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        return doc.features[name]!!
    }
}

