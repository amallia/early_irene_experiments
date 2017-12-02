package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.iltr.RREnv
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.irene.scoring.IreneQueryScorer
import edu.umass.cics.ciir.irene.scoring.MultiEvalNode
import edu.umass.cics.ciir.irene.scoring.QueryEvalNode
import edu.umass.cics.ciir.sprf.DataPaths
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.Collector
import org.apache.lucene.search.CollectorManager
import org.apache.lucene.search.LeafCollector
import org.apache.lucene.search.Scorer
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author jfoley
 */

/**
 * Imagine a [TextExpr] that you wanted to always return zero, for some reason. Used to estimate the lower-bound of SDM's dependencies.
 */
fun MissingTermScoreHack(t: String, env: RREnv): QExpr {
    return ConstCountExpr(0, LengthsExpr(env.defaultField, stats=env.getStats(t)))
}
fun main(args: Array<String>) {
    val dataset = DataPaths.get("robust")
    val sdm_uw = 0.8
    val sdm_odw = 0.15
    val sdm_uww = 0.05

    val totalOffered = StreamingStats()
    val totalPlausible = StreamingStats()
    dataset.getIreneIndex().use { index ->
        index.env.estimateStats = "min"
        dataset.title_qs.forEach { qid, qtext ->
            val qterms = index.tokenize(qtext)

            // Optimization does nothing for queries with single term.
            if (qterms.size <= 1) return@forEach

            val meanW = 1.0 / 3.0
            val ql = QueryLikelihood(qterms).weighted(sdm_uw).weighted(meanW)
            val bestCaseWindowEstimators = ArrayList<QExpr>()
            val worstCaseWindowEstimators = ArrayList<QExpr>()
            qterms.forEachSeqPair { lhs, rhs ->
                bestCaseWindowEstimators.add(DirQLExpr(MinCountExpr(listOf(TextExpr(lhs), TextExpr(rhs)))))
                worstCaseWindowEstimators.add(DirQLExpr(
                        MinCountExpr(listOf(lhs, rhs).map { MissingTermScoreHack(it, index.env) })
                ))
            }
            val baseExpr = MeanExpr(bestCaseWindowEstimators)
            val odEst = baseExpr.copy().weighted(sdm_odw).weighted(meanW)
            val uwEst = baseExpr.copy().weighted(sdm_uww).weighted(meanW)

            val badBaseExpr = MeanExpr(worstCaseWindowEstimators)
            val odEstBad = badBaseExpr.copy().weighted(sdm_odw).weighted(meanW)
            val uwEstBad = badBaseExpr.copy().weighted(sdm_uww).weighted(meanW)

            val bestCase = SumExpr(odEst, uwEst)
            val maxMinExpr = MultiExpr(mapOf("base" to ql, "best" to bestCase, "worst" to SumExpr(odEstBad, uwEstBad)))

            val lq = index.prepare(maxMinExpr)
            val poolTarget = 1000
            val results = index.searcher.search(lq, MaxMinCollectorManager(maxMinExpr, poolTarget))
            println("$qid $qterms ${results.totalHits} -> ${results.totalOffered} -> ${results.prunedHits.size} -> ${poolTarget}")
            totalOffered.push(results.totalOffered)
            totalPlausible.push(results.prunedHits.size)
        }

        System.out.println("offered: ${totalOffered}")
        System.out.println("plausible: ${totalPlausible}")
        System.out.println("pl/off: ${totalPlausible.mean / totalOffered.mean}")
    }
}

fun <T> List<T>.findIndex(x: T): Int? {
    val pos = this.indexOf(x)
    if (pos >= 0) return pos
    return null
}

// Keep in heap based on the "worst" possible score for each document.
data class MaxMinScoreDoc(val minScore: Float, val maxScore: Float, val id: Int): ScoredForHeap {
    override val score: Float get() = minScore
}
data class MaxMinResults(val totalHits: Int, val totalOffered: Long, val prunedHits: List<MaxMinScoreDoc>)
class MaxMinCollectorManager(val mq: MultiExpr, val poolSize: Int): CollectorManager<MaxMinCollectorManager.MaxMinCollector, MaxMinResults> {
    override fun reduce(collectors: Collection<MaxMinCollector>): MaxMinResults {
        val plausible = ArrayList<MaxMinScoreDoc>()
        for (c in collectors) {
            plausible.addAll(c.plausibleCandidates)
        }
        if (heap.isEmpty()) return MaxMinResults(0, 0L, emptyList())
        val worst = heap.min
        println(heap.unsortedList.sample(10))
        println("Worst-case: $worst, total-plausible: ${plausible.size}")
        return MaxMinResults(totalHits.get(), heap.totalSeen, plausible.filter { it.maxScore >= worst })
    }
    override fun newCollector(): MaxMinCollector = MaxMinCollector()

    // When the plausibleCandidatesPool gets to 3k, let's drop any items that can't possibly fit.
    val pruneSize = poolSize*3
    val names = mq.names
    val baseIndex = names.findIndex("base") ?: error("No base expression available.")
    val bestIndex = names.findIndex("best") ?: error("No best expression available.")
    val worstIndex = names.findIndex("worst") ?: error("No worst expression available.")
    val heap = ScoringHeap<MaxMinScoreDoc>(poolSize)
    val totalHits = AtomicInteger(0)

    inner class MaxMinCollector : Collector {
        val plausibleCandidates = ArrayList<MaxMinScoreDoc>()

        override fun needsScores(): Boolean = true
        override fun getLeafCollector(context: LeafReaderContext): LeafCollector {
            var localMin = -Float.MAX_VALUE
            val docBase = context.docBase
            return object : LeafCollector {
                lateinit var baseExpr: QueryEvalNode
                lateinit var bestExpr: QueryEvalNode
                lateinit var worstExpr: QueryEvalNode
                override fun setScorer(scorer: Scorer) {
                    val eval = (scorer as IreneQueryScorer).eval as MultiEvalNode
                    baseExpr = eval.children[baseIndex]
                    bestExpr = eval.children[bestIndex]
                    worstExpr = eval.children[worstIndex]
                }
                override fun collect(docNo: Int) {
                    val gdoc = docNo + docBase
                    totalHits.incrementAndGet()

                    // Get bounds on expression.
                    val base = baseExpr.score(docNo)
                    val best = bestExpr.score(docNo)
                    val worst = worstExpr.score(docNo)
                    val max = base + best
                    val min = base + worst

                    if (java.lang.Float.isInfinite(min)) {
                        println(worstExpr.explain(docNo))
                        return
                    }

                    // Ditch any that aren't plausible in the best case scenario.
                    // This is where we save on calculating real term dependencies.
                    if (max < localMin) {
                        return
                    }

                    // Try and put it in the heap then (may bump the localMin up if it kicks out another)
                    val doc = MaxMinScoreDoc(min, max, gdoc)
                    plausibleCandidates.add(doc)
                    val changed = synchronized(heap) {
                        heap.offer(doc)
                        if (heap.isFull) {
                            localMin = maxOf(localMin, heap.min)
                            true
                        } else false
                    }

                    // localMin has increased: let us prune our pool if necessary.
                    if (changed && plausibleCandidates.size > pruneSize) {
                        val orig = plausibleCandidates.size
                        val stillValid = plausibleCandidates.filter { it.maxScore > localMin }
                        // If we found things to delete, do so.
                        if (stillValid.size != plausibleCandidates.size) {
                            // swap into:
                            plausibleCandidates.clear()
                            plausibleCandidates.addAll(stillValid)
                            //println("$orig -> ${plausibleCandidates.size}")
                        }
                    }
                }
            }
        }

    }
}