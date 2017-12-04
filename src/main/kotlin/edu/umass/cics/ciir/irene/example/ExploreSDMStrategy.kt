package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.iltr.RREnv
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.irene.scoring.IreneQueryScorer
import edu.umass.cics.ciir.irene.scoring.MultiEvalNode
import edu.umass.cics.ciir.irene.scoring.QueryEvalNode
import edu.umass.cics.ciir.sprf.DataPaths
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.*
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
    val dataset = DataPaths.get("gov2")
    val sdm_uw = 0.8
    val sdm_odw = 0.15
    val sdm_uww = 0.05
    val poolTarget = 1000
    val meanW = 1.0 / 3.0

    val baseTimeStats = StreamingStats()
    val approxTimeStats = StreamingStats()
    val totalOffered = StreamingStats()
    val totalPlausible = StreamingStats()
    dataset.getIreneIndex().use { index ->
        index.env.estimateStats = "min"
        dataset.title_qs.forEach { qid, qtext ->
            val qterms = index.tokenize(qtext)

            // Optimization does nothing for queries with single term.
            if (qterms.size <= 1) return@forEach

            val ql = QueryLikelihood(qterms).weighted(sdm_uw).weighted(meanW)
            val bestCaseWindowEstimators = ArrayList<QExpr>()
            val worstCaseWindowEstimators = ArrayList<QExpr>()
            val odWindows = ArrayList<QExpr>()
            val uwWindows = ArrayList<QExpr>()
            qterms.forEachSeqPair { lhs, rhs ->
                bestCaseWindowEstimators.add(DirQLExpr(MinCountExpr(listOf(TextExpr(lhs), TextExpr(rhs)))))
                worstCaseWindowEstimators.add(DirQLExpr(
                        MinCountExpr(listOf(lhs, rhs).map { MissingTermScoreHack(it, index.env) })
                ))
                odWindows.add(DirQLExpr(OrderedWindowExpr(listOf(lhs, rhs).map { TextExpr(it) })))
                uwWindows.add(DirQLExpr(UnorderedWindowExpr(listOf(lhs, rhs).map { TextExpr(it) })))
            }
            val baseExpr = MeanExpr(bestCaseWindowEstimators)
            val odEst = baseExpr.copy().weighted(sdm_odw).weighted(meanW)
            val uwEst = baseExpr.copy().weighted(sdm_uww).weighted(meanW)

            val badBaseExpr = MeanExpr(worstCaseWindowEstimators)
            val odEstBad = badBaseExpr.copy().weighted(sdm_odw).weighted(meanW)
            val uwEstBad = badBaseExpr.copy().weighted(sdm_uww).weighted(meanW)

            val bestCase = SumExpr(odEst, uwEst)
            val maxMinExpr = MultiExpr(mapOf(
                    "base" to ql,
                    "best" to bestCase,
                    "worst" to SumExpr(odEstBad, uwEstBad),
                    "true" to SumExpr(
                            MeanExpr(odWindows).weighted(sdm_odw).weighted(meanW),
                            MeanExpr(uwWindows).weighted(sdm_uww).weighted(meanW))
            ))

            val sdmQ = SequentialDependenceModel(qterms)

            val (baseTime, expectedDocs) = timed { index.search(sdmQ, poolTarget) }
            baseTimeStats.push(baseTime)

            val expected = expectedDocs.scoreDocs.mapTo(HashSet()) { it.doc }

            val lq = index.prepare(maxMinExpr)
            val (approxTime, results) = timed { index.searcher.search(lq, MaxMinCollectorManager(maxMinExpr, poolTarget)) }
            approxTimeStats.push(approxTime)
            //println("$qid $qterms ${results.totalHits} -> ${results.totalOffered} -> ${results.prunedHits.size} -> ${poolTarget}")
            //totalOffered.push(results.totalOffered)
            //totalPlausible.push(results.prunedHits.size)

            val returned = results.prunedHits.mapTo(HashSet()) { it.id }
            //val returned = results.scoreDocs.mapTo(HashSet()) { it.doc }

            val recall = Recall(expected, returned)
            println("Recall vs. True SDM: $recall ... ${recall.value}")
            println("${(approxTime*1000).toInt()}ms vs. full ${(baseTime*1000).toInt()}ms")
        }

        println("offered: ${totalOffered}")
        println("plausible: ${totalPlausible}")
        println("pl/off: ${totalPlausible.mean / totalOffered.mean}")
        println()
        println("SDM-time: ${baseTimeStats}")
        println("Approx-time: ${approxTimeStats}")
    }
}

// Keep in heap based on the "worst" possible score for each document.
data class MaxMinScoreDoc(val minScore: Float, val baseScore: Float, val maxScore: Float, val id: Int): ScoredForHeap {
    override val score: Float get() = minScore
}
data class MaxMinResults(val totalHits: Int, val totalOffered: Long, val prunedHits: List<MaxMinScoreDoc>) {
    var exactResults: TopDocs? = null
}
class MaxMinCollectorManager(val mq: MultiExpr, val poolSize: Int): CollectorManager<MaxMinCollectorManager.MaxMinCollector, MaxMinResults> {
    override fun reduce(collectors: Collection<MaxMinCollector>): MaxMinResults {
        val plausible = ArrayList<MaxMinScoreDoc>()
        for (c in collectors) {
            plausible.addAll(c.plausibleCandidates)
        }
        if (heap.isEmpty()) return MaxMinResults(0, 0, emptyList())
            //return TopDocs(0, emptyArray(), -Float.MAX_VALUE)
        val worst = heap.min
        //println(heap.unsortedList.sample(10))
        //println("Worst-case: $worst, total-plausible: ${plausible.size}")

        // Finish scoring exactly.
        val candidates = plausible.filter { it.maxScore >= worst }

        //println("\t${totalHits.get()} -> ${heap.totalSeen} -> ${candidates.size} -> ${poolSize}")

        //val exactHeap = ScoringHeap<IreneScoredDoc>(poolSize)
        //for (c in collectors) {
            //c.finishScoring(candidates, exactHeap)
        //}

        //return toTopDocs(exactHeap, totalHits.get().toLong())
        return MaxMinResults(totalHits.get(), heap.totalSeen, candidates)
    }
    override fun newCollector(): MaxMinCollector = MaxMinCollector()

    // When the plausibleCandidatesPool gets to 3k, let's drop any items that can't possibly fit.
    val pruneSize = poolSize*3
    val names = mq.names
    val baseIndex = names.findIndex("base") ?: error("No base expression available.")
    val bestIndex = names.findIndex("best") ?: error("No best expression available.")
    val worstIndex = names.findIndex("worst") ?: error("No worst expression available.")
    val trueIndex = names.findIndex("true") ?: error("No true expression available.")
    val heap = ScoringHeap<MaxMinScoreDoc>(poolSize)
    val totalHits = AtomicInteger(0)

    inner class MaxMinLeafCollector(val plausibleCandidates: ArrayList<MaxMinScoreDoc>, val docBase: Int, val numDocs: Int) : LeafCollector {
        var localMin = -Float.MAX_VALUE
        lateinit var baseExpr: QueryEvalNode
        lateinit var bestExpr: QueryEvalNode
        lateinit var worstExpr: QueryEvalNode
        lateinit var trueExpr: QueryEvalNode
        override fun setScorer(scorer: Scorer) {
            val eval = (scorer as IreneQueryScorer).eval as MultiEvalNode
            baseExpr = eval.children[baseIndex]
            bestExpr = eval.children[bestIndex]
            worstExpr = eval.children[worstIndex]
            trueExpr = eval.children[trueIndex]
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
            val doc = MaxMinScoreDoc(min, base, max, gdoc)
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

        fun finishScoring(relevant: List<MaxMinScoreDoc>, heap: ScoringHeap<IreneScoredDoc>) {
            for (mmsd in relevant) {
                if (heap.isFull && mmsd.maxScore < heap.min) {
                    continue
                }
                val doc = mmsd.id - docBase
                val delta = trueExpr.score(doc)
                val exactScore = mmsd.baseScore + delta

                assert(exactScore >= mmsd.minScore && exactScore <= mmsd.maxScore) {
                    println("$mmsd")
                    println(" + $delta = $exactScore")
                }

                heap.offer(exactScore, {IreneScoredDoc(exactScore, mmsd.id)})
            }
        }
    }

    inner class MaxMinCollector : Collector {
        val plausibleCandidates = ArrayList<MaxMinScoreDoc>()
        val leafCollectors = HashMap<Any, MaxMinLeafCollector>()

        override fun needsScores(): Boolean = true
        override fun getLeafCollector(context: LeafReaderContext): LeafCollector {
            val docBase = context.docBase
            val lc = MaxMinLeafCollector(plausibleCandidates, docBase, context.reader().numDocs())
            leafCollectors[context.id()] = lc
            return lc
        }

        fun finishScoring(candidates: List<MaxMinScoreDoc>, exactHeap: ScoringHeap<IreneScoredDoc>) {
            println("${candidates.size}")
            leafCollectors.values.sortedBy { it.docBase }.forEach { mmlc ->
                val range = (mmlc.docBase until mmlc.docBase+mmlc.numDocs)
                val relevant = candidates.filter { range.contains(it.id) }.sortedBy { it.id }
                println("${mmlc.docBase}: ${relevant.size}")
                mmlc.finishScoring(relevant, exactHeap)
            }
            leafCollectors.clear()
        }

    }
}