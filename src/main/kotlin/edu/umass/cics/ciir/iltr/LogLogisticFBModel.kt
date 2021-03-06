package edu.umass.cics.ciir.iltr

import com.github.benmanes.caffeine.cache.Caffeine
import edu.umass.cics.ciir.irene.galago.RRGalagoEnv
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.ltr.RREnv
import edu.umass.cics.ciir.irene.ltr.RRLogLogisticTFScore
import edu.umass.cics.ciir.irene.ltr.WeightedTerm
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.irene.galago.getEvaluators
import edu.umass.cics.ciir.irene.utils.*
import org.lemurproject.galago.utility.Parameters
import java.util.*
import kotlin.collections.HashMap

/**
 *
 * @author jfoley.
 */
data class LLHyperParam(val fbDocs: Int, var fbTerms: Int, val c: Double, var lambda: Double = 0.5) : HyperParam
val msg = Debouncer()
fun computePMI(env: RREnv, w1: String, w2: String): Double {
    if (w1 == w2) {
        val p = env.getStats(w1).binaryProbability()
        // = p * log (p/p*p) = p * log(1/p) = p * -log(p)
        return -p * Math.log(p)
    }

    val denom = env.getStats(w1).binaryProbability() * env.getStats(w2).binaryProbability()

    val isectQ = AndExpr(listOf(TextExpr(w1, w2)))
    if (msg.ready()) {
        println("\t\t\tcomputePMI $w1 $w2")
    }

    val isectStats = env.computeStats(isectQ)

    val p = isectStats.binaryProbability()

    return p * Math.log(p / denom)
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "robust")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val tuningMeasure = evals["ap"]!!
    val qrels = dataset.getQueryJudgments()
    val field = argp.get("field", "document")
    val fbDocsN = argp.get("fbDocs", 10)
    val fbTermsN = argp.get("fbTerms", 50)
    val sweepLambdas = argp.get("sweepLambdas", false)
    val sweepCValues = argp.get("sweepCValues", false)
    val sweepFBTerms = argp.get("sweepFBTerms", false)

    // hyperparam -> tuningMeasure
    val hpResults = HashMap<HyperParam, HashMap<String, Double>>()

    val kSplits = 5
    val splitToQids = HashMap<Int, MutableList<String>>()
    dataset.title_qs.keys.sorted().forEachIndexed { i, qid ->
        val splitId = i % kSplits
        splitToQids.push(splitId, qid)
    }

    val pmiCache = Caffeine.newBuilder().maximumSize(100_000).build<Pair<String,String>, Double>()

    dataset.getIndex().use { retr ->
        val env = RRGalagoEnv(retr)
        val computePMI: (Pair<String,String>)->Double = { (w1, w2) ->
            computePMI(env, w1, w2)
        }
        forEachQuery(dsName) { q ->
            //if (q.qid.toInt() > 310) return@forEachQuery
            val queryJudgments = qrels[q.qid]
            println("${q.qid} ${q.qterms}")

            val fbDocs = q.docs.take(fbDocsN)
            val terms = fbDocs.flatMapTo(HashSet()) { it.terms(field) }

            val fbTerms = HashMap<LLHyperParam, ArrayList<WeightedTerm>>()

            terms.forEach { term ->
                val AllCValues = RRLogLogisticTFScore.OriginalPaperCValues

                val sem = q.qterms.sumByDouble { q ->
                    val key = ReflexivePair(q, term)
                    val s_wq = pmiCache.get(key, computePMI)!!
                    val s_qq = pmiCache.get(ReflexivePair(q,q), computePMI)!!
                    s_wq / s_qq
                }

                if (sweepCValues) { AllCValues } else { listOf(7.0) }
                        .forEach { c ->
                            val params = LLHyperParam(fbDocsN, fbTermsN, c)
                            val llExpr = RRLogLogisticTFScore(env, term, field, c)
                            val priorExpr = env.feature("title-ql-prior")

                            val fwExpr = env.mult(llExpr, priorExpr, env.const(sem))
                            val fw = fbDocs.meanByDouble { fwExpr.eval(it) }

                            fbTerms
                                    .computeIfAbsent(params, {ArrayList()})
                                    .add(WeightedTerm(fw, term))
                        }
            }

            println("\tComputed fbTerms for ${fbTerms.size} settings.")

            val expQueries = HashMap<LLHyperParam, QExpr>()
            fbTerms.forEach { (params, wts) ->
                val fbTermsOpts = arrayListOf(10,20,50,100)
                if (sweepFBTerms) { fbTermsOpts } else { listOf(params.fbTerms) }
                        .forEach { numT ->
                            val hp = params.copy(fbTerms = numT)
                            val bestK = wts.sorted().take(hp.fbTerms).associate { Pair(it.term, it.score) }.normalize()

                            val expr = SumExpr(bestK.map { (term,score) -> DirQLExpr(TextExpr(term)).weighted(score) })
                            expQueries.put(hp, expr)
                        }
            }

            println("\tComputed expQueries for ${expQueries.size} settings.")

            expQueries.forEach { params, expQ ->
                val allLambdas = (0..10).map {it/10.0}

                if (sweepLambdas) { allLambdas } else { listOf(0.3) }
                        .forEach { lambda ->
                            val hp = params.copy(lambda=lambda)
                            val fullQ = QueryLikelihood(q.qterms).mixed(hp.lambda, expQ)
                            //println("\t\t\t$fullQ")
                            val ranked = q.toQResults(env, fullQ)
                            val score = tuningMeasure.evaluate(ranked, queryJudgments)
                            //println("\t\t$score . $hp")
                            hpResults.computeIfAbsent(hp,{ HashMap() }).put(q.qid, score)
                        }
            }
            println("\tComputed gain for ${hpResults.size} settings.")
        }
    }
    println("Done computation.")


    val testMean = splitToQids.map { (splitId, test) ->
        val train = splitToQids.filterKeys { it != splitId }.values.flatten()

        val (bestHP, bestTrainScore) = hpResults.map { (hp, items) ->
            val trainMeasure = train.map { items[it]!! }.mean()
            Pair(hp, trainMeasure)
        }.sortedByDescending { it.second }.first()

        val bestItems = hpResults[bestHP]!!
        val testScore = test.map { bestItems[it]!! }.mean()

        println("Split $splitId: Train: $bestTrainScore for $bestHP, Test: $testScore")
        testScore
    }.mean()

    // Condensed-List Regular LL, with DirQL eval: AP=0.269
    // CL LL+Rel: AP=0.277 (noSweep)
    // CL LL+Rel+Sem: AP=0.249 (withSweep)
    println("Overall Test Mean: $testMean")
}
