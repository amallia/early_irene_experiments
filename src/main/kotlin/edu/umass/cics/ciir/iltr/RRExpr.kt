package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.sprf.GExpr
import edu.umass.cics.ciir.sprf.cfProbability
import org.lemurproject.galago.core.retrieval.LocalRetrieval

/**
 * @author jfoley
 */
class RREnv(val retr: LocalRetrieval) {
    val mu = 1500.0
    val bm25b = 0.75
    val bm25k = 1.2
    val lengths = retr.getCollectionStatistics(GExpr("lengths"))!!

    fun mean(exprs: List<RRExpr>) = RRMean(this, exprs)
    fun mean(vararg exprs: RRExpr) = RRMean(this, exprs.toList())
    fun sum(exprs: List<RRExpr>) = RRSum(this, exprs)
    fun sum(vararg exprs: RRExpr) = RRSum(this, exprs.toList())
    fun term(term: String) = RRDirichletTerm(this, term)
    fun feature(name: String) = RRFeature(this, name)

    fun ql(terms: List<String>) = mean(terms.map { RRDirichletTerm(this, it) })
    fun bm25(terms: List<String>) = sum(terms.map { RRBM25Term(this, it) })

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

class RRBM25Term(env: RREnv, val term: String, val b: Double = env.bm25b, val k: Double = env.bm25k) : RRLeafExpr(env) {
    val nstats = env.retr.getNodeStatistics(GExpr("counts", term))
    val avgDL = env.lengths.avgLength;
    val df = nstats.nodeDocumentCount;
    val dc = env.lengths.documentCount;

    val idf = Math.log(dc / (df + 0.5))

    override fun eval(doc: LTRDoc): Double {
        val count = doc.freqs.count(term).toDouble()
        val length = doc.freqs.length
        val num = count * (k+1.0)
        val denom = count + (k * (1.0 - b + (b * length / avgDL)))
        return idf * num / denom
    }
}

class RRAvgWordLength(env: RREnv) : RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        var sum = 0.0
        var n = 0
        doc.freqs.counts.forEachEntry { term, count ->
            sum += (term.length * count).toDouble()
            n += count
            true
        }
        return sum / n
    }
}

class RRDocLength(env: RREnv) : RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double = doc.freqs.length
}

// Return the index of a term in a document as a fraction: for news, this should be similar to importance. A query term at the first position will receive a 1.0; halfway through the document will receive a 0.5.
class RRTermPosition(env: RREnv, val term: String): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        val length = doc.terms.size.toDouble()
        val pos = doc.terms.indexOf(term).toDouble()
        if (pos < 0) {
            return 0.0
        } else {
            return 1.0 - (pos / length)
        }
    }
}

class RRJaccardSimilarity(env: RREnv, val target: Set<String>, val empty: Double=0.5): RRLeafExpr(env) {
    override fun eval(doc: LTRDoc): Double {
        if (target.isEmpty()) return empty
        val uniq = doc.freqs.counts.keys().toSet()
        if (uniq.isEmpty()) return empty
        val overlap = (uniq intersect target).size.toDouble()
        val domain = (uniq union target).size.toDouble()
        return overlap / domain
    }
}