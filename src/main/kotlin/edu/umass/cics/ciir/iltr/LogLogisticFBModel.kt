package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.dbpedia.normalize
import edu.umass.cics.ciir.mean
import edu.umass.cics.ciir.push
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.getEvaluators
import org.lemurproject.galago.utility.Parameters
import java.util.*
import kotlin.collections.HashMap

/**
 *
 * @author jfoley.
 */
data class HyperParam(val fbDocs: Int, var fbTerms: Int, val c: Double, var lambda: Double = 0.5)

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "robust")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val tuningMeasure = evals["ndcg"]!!
    val qrels = dataset.getQueryJudgments()
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

    dataset.getIndex().use { retr ->
        val env = RREnv(retr)
        forEachQuery(dsName) { q ->
            //if (q.qid.toInt() > 310) return@forEachQuery
            val queryJudgments = qrels[q.qid]
            println("${q.qid} ${q.qterms}")

            val fbDocs = q.docs.take(fbDocsN)
            val terms = fbDocs.flatMapTo(HashSet<String>()) { it.terms }

            val fbTerms = HashMap<HyperParam, ArrayList<WeightedTerm>>()

            terms.forEach { term ->
                val AllCValues = RRLogLogisticTFScore.OriginalPaperCValues

                if (sweepCValues) { AllCValues } else { listOf(7.0) }
                        .forEach { c ->
                            val params = HyperParam(fbDocsN, fbTermsN, c)
                            val fwExpr = RRLogLogisticTFScore(env, term, c)
                            var sum = 0.0
                            fbDocs.forEach { doc ->
                                sum += fwExpr.eval(doc)
                            }
                            val fw = sum / fbDocs.size.toDouble()

                            fbTerms
                                    .computeIfAbsent(params, {ArrayList<WeightedTerm>()})
                                    .add(WeightedTerm(fw, term))
                        }
            }

            println("\tComputed fbTerms for ${fbTerms.size} settings.")

            val expQueries = HashMap<HyperParam, RRExpr>()
            fbTerms.forEach { (params, wts) ->
                val fbTermsOpts = arrayListOf<Int>(10,20,50,100)
                if (sweepFBTerms) { fbTermsOpts } else { listOf(params.fbTerms) }
                        .forEach { numT ->
                            val hp = params.copy(fbTerms = numT)
                            val bestK = wts.sorted().take(hp.fbTerms).associate { Pair(it.term, it.score) }.normalize()
                            val expr = env.sum(bestK.map { (term, score) -> RRDirichletTerm(env, term).weighted(score) })
                            expQueries.put(hp, expr)
                        }
            }

            println("\tComputed expQueries for ${expQueries.size} settings.")

            expQueries.forEach { params, expQ ->
                val allLambdas = (0..10).map {it/10.0}

                if (sweepLambdas) { allLambdas } else { listOf(0.3) }
                        .forEach { lambda ->
                            val hp = params.copy(lambda=lambda)
                            val fullQ =
                                    env.ql(q.qterms).mixed(hp.lambda, expQ)
                            //println("\t\t\t$fullQ")
                            val ranked = q.toQResults(fullQ)
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

    println("Overall Test Mean: $testMean")
}
