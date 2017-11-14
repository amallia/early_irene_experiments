package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.irene.scoring.approxStats
import edu.umass.cics.ciir.sprf.inqueryStop
import gnu.trove.map.hash.TObjectDoubleHashMap

abstract class RREnv {
    open var defaultField = "document"
    open var defaultDirichletMu = 1500.0
    open var defaultBM25b = 0.75
    open var defaultBM25k = 1.2
    open var absoluteDiscountingDelta = 0.7
    open var estimateStats: String? = "min"

    abstract fun computeStats(q: QExpr): CountStats
    abstract fun getStats(term: String, field: String? =null): CountStats
    fun statsComputation(q: QExpr): CountStatsStrategy {
        if (q is OrderedWindowExpr || q is UnorderedWindowExpr) {
            if (estimateStats == null || estimateStats == "exact") {
                return RREnvLazyStats(this, q.copy())
            } else {
                return approxStats(q, estimateStats!!)
            }
        } else {
            TODO("statsComputation($q)")
        }
    }
    fun fromQExpr(q: QExpr): RRExpr = when(q) {
        is TextExpr -> RRTermExpr(this, q.text, q.field ?: defaultField)
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
        is OrderedWindowExpr -> RROrderedWindow(this,
                q.children.map { fromQExpr(it) as RRCountExpr },
                q.step,
                statsComputation(q))
        is UnorderedWindowExpr -> RRUnorderedWindow(this,
                q.children.map { fromQExpr(it) as RRCountExpr },
                q.width,
                statsComputation(q))
        is WeightExpr -> RRWeighted(this, q.weight, fromQExpr(q.child))
        is DirQLExpr -> RRDirichletScorer(this, fromQExpr(q.child) as RRCountExpr, q.mu ?: defaultDirichletMu)
        is BM25Expr -> RRBM25Scorer(this, fromQExpr(q.child) as RRCountExpr, q.b ?: defaultBM25b, q.k ?: defaultBM25k)
        is CountToScoreExpr -> TODO()
        is BoolToScoreExpr -> TODO()
        is CountToBoolExpr -> TODO()
        is RequireExpr -> TODO()
        is ConstScoreExpr -> RRConst(this, q.x)
        is ConstCountExpr -> RRConst(this, q.x.toDouble())
        is ConstBoolExpr -> RRConst(this, if (q.x) 1.0 else 0.0)
    }


    fun mean(exprs: List<RRExpr>) = RRMean(this, exprs)
    fun mean(vararg exprs: RRExpr) = RRMean(this, exprs.toList())
    fun sum(exprs: List<RRExpr>) = RRSum(this, exprs)
    fun sum(vararg exprs: RRExpr) = RRSum(this, exprs.toList())
    fun term(term: String) = RRDirichletTerm(this, term)
    fun feature(name: String) = RRFeature(this, name)

    fun ql(terms: List<String>) = mean(terms.map { RRDirichletTerm(this, it) })
    fun bm25(terms: List<String>, field: String = defaultField) = sum(terms.map { RRBM25Term(this, it, field) })
    fun mult(vararg exprs: RRExpr) = RRMult(this, exprs.toList())
    fun const(x: Double) = RRConst(this, x)


    fun computeRelevanceModel(docs: List<LTRDoc>, feature: String, fbDocs: Int, flat: Boolean = false, stopwords: Set<String> = inqueryStop, field: String = defaultField): RelevanceModel {
        val fbdocs = docs.sortedByDescending {
            it.features[feature] ?: error("Missing Feature! $feature in $it")
        }.take(fbDocs)

        val rmModel = TObjectDoubleHashMap<String>()
        fbdocs.forEach { doc ->
            val local = doc.field(field).freqs.counts
            val length = doc.field(field).freqs.length

            val prior = if (flat) 1.0 else doc.features[feature]!!
            local.forEachEntry {term, count ->
                if (stopwords.contains(term)) return@forEachEntry true
                val prob = prior * count.toDouble() / length
                rmModel.adjustOrPutValue(term, prob, prob)
                true
            }
        }

        return RelevanceModel(rmModel, field)
    }
}

data class WeightedTerm(val score: Double, val term: String, val field: String = "") : Comparable<WeightedTerm> {
    // Natural order: biggest first.
    override fun compareTo(other: WeightedTerm): Int {
        var cmp = -java.lang.Double.compare(score, other.score)
        if (cmp != 0) return cmp
        cmp = field.compareTo(other.field)
        if (cmp != 0) return cmp
        return term.compareTo(other.term)
    }
}

data class RelevanceModel(val weights: TObjectDoubleHashMap<String>, val sourceField: String) {
    fun toTerms(): List<WeightedTerm> {
        val output = ArrayList<WeightedTerm>(weights.size())
        weights.forEachEntry {term, weight ->
            output.add(WeightedTerm(weight, term, sourceField))
        }
        return output
    }
    fun toTerms(k: Int): List<WeightedTerm> = toTerms().sorted().take(k).normalized()
    fun toQExpr(k: Int, scorer: (TextExpr)->QExpr = {DirQLExpr(it)}, targetField: String? = null) = SumExpr(toTerms(k).map { scorer(TextExpr(it.term, targetField ?: sourceField)).weighted(it.score) })
}
