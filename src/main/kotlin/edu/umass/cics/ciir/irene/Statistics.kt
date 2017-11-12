package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.irene.scoring.IreneQueryModel
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.index.Term
import org.apache.lucene.index.TermContext
import org.apache.lucene.search.*

/**
 *
 * @author jfoley.
 */
data class CountStats(var text: String, var cf: Long, var df: Long, var cl: Long, var dc: Long) {
    constructor(text: String): this(text, 0,0,0,0)
    constructor(text: String, termStats: TermStatistics?, cstats: CollectionStatistics) : this(text,
            cf=termStats?.totalTermFreq() ?: 0,
            df=termStats?.docFreq() ?: 0,
            cl=cstats.sumTotalTermFreq(),
            dc=cstats.docCount())

    fun avgDL() = cl.toDouble() / dc.toDouble();
    fun countProbability() = cf.toDouble() / cl.toDouble()
    fun nonzeroCountProbability() = Math.max(0.5,cf.toDouble()) / cl.toDouble()
    fun binaryProbability() = df.toDouble() / dc.toDouble()
    operator fun plusAssign(rhs: CountStats?) {
        if (rhs != null) {
            cl += rhs.cl
            df += rhs.df
            cf += rhs.cf
            dc += rhs.dc
        }
    }
}

abstract class CountStatsStrategy {
    abstract fun get(): CountStats
}
class LazyCountStats(val expr: QExpr, val index: IreneIndex) : CountStatsStrategy() {
    private val stats: CountStats by lazy { index.getStats(expr)!! }
    override fun get(): CountStats = stats
}
inline fun <T> List<T>.lazyIntMin(func: (T)->Int): Int? {
    var curMin: Int? = null
    for (x in this) {
        val cur = func(x)
        if (curMin == null || (curMin > cur)) {
            curMin = cur
        }
    }
    return curMin
}
inline fun <T> List<T>.lazyMinAs(func: (T)->Long): Long? {
    var curMin: Long? = null
    for (x in this) {
        val cur = func(x)
        if (curMin == null || (curMin > cur)) {
            curMin = cur
        }
    }
    return curMin
}
class MinEstimatedCountStats(expr: QExpr, cstats: List<CountStats>): CountStatsStrategy() {
    override fun get(): CountStats = estimatedStats
    private val estimatedStats = CountStats("Est($expr)",
            cstats.lazyMinAs { it.cf }?:0,
            cstats.lazyMinAs { it.df }?:0, cstats[0].cl, cstats[0].dc)
}
class ProbEstimatedCountStats(expr: QExpr, cstats: List<CountStats>): CountStatsStrategy() {
    override fun get(): CountStats = estimatedStats
    private val estimatedStats = CountStats("Est($expr)",0,0,cstats[0].cl, cstats[0].dc).apply {
        if (cstats.lazyMinAs { it.cf } ?: 0L == 0L) {
            // one term does not exist, all things are zero.
        } else {
            cf = Math.exp(
                    // size of collection
                    Math.log(cl.toDouble()) +
                    // multiply probabilities together in logspace / term-independence model
                    cstats.map { Math.log(it.countProbability()) }.sum()).toLong()
            df = Math.exp(
                    // size of collection (# docs)
                    Math.log(df.toDouble()) +
                    // multiply probabilities together in logspace / term-independence model
                    cstats.map { Math.log(it.binaryProbability()) }.sum()).toLong()
        }
    }
}

class CountStatsCollectorManager(val start: CountStats) : CollectorManager<CountStatsCollectorManager.CountStatsCollector, CountStats> {
    override fun reduce(collectors: MutableCollection<CountStatsCollector>?): CountStats {
        val out = start.copy()
        collectors?.forEach {
            out += it.stats
        }
        return out
    }

    override fun newCollector(): CountStatsCollector = CountStatsCollector()
    class CountStatsLeafCollector(val docBase: Int, val accum: CountStats) : LeafCollector {
        lateinit var scoreFn: Scorer
        override fun setScorer(scorer: Scorer?) { scoreFn = scorer!! }

        override fun collect(doc: Int) {
            val score = scoreFn.score()
            val count = score.toInt()
            assert(score - count < 1e-10, {"Collecting count stats but got float score: ${docBase+doc} -> $score -> $count"})

            if (count > 0) {
                accum.cf += count
                accum.df += 1
            }
        }

    }
    class CountStatsCollector : Collector {
        val stats = CountStats("tmp:CountStatsCollector")
        override fun needsScores(): Boolean = false
        override fun getLeafCollector(context: LeafReaderContext?): LeafCollector = CountStatsLeafCollector(context!!.docBase, stats)
    }
}


object CalculateStatistics {
    fun lookupTermStatistics(searcher: IndexSearcher, term: Term): CountStats? {
        val cstats = searcher.collectionStatistics(term.field())
        val ctx = TermContext.build(searcher.topReaderContext, term) ?: return null
        val termStats = searcher.termStatistics(term, ctx) ?: return null
        return CountStats("term:$term", termStats, cstats)
    }

    fun fieldStats(searcher: IndexSearcher, field: String): CountStats? {
        val cstats = searcher.collectionStatistics(field) ?: return null
        return CountStats("field:$field", null, cstats)
    }

    fun computeQueryStats(searcher: IndexSearcher, query: IreneQueryModel): CountStats {
        val fields = query.findFieldsNeeded()

        val fieldBasedStats = CountStats("expr:${query.exec}")
        fields.forEach { field ->
            val fstats = fieldStats(searcher, field) ?: error("Field: ``$field'' does not exist in index.")
            fieldBasedStats.dc = maxOf(fstats.dc, fieldBasedStats.dc)
            fieldBasedStats.cl += fstats.cl
        }
        return searcher.search(query, CountStatsCollectorManager(fieldBasedStats))
    }
}