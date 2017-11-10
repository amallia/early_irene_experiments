package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluators
import org.lemurproject.galago.utility.Parameters

/**
 *
 * @author jfoley.
 */
data class HyperParam(val fbDocs: Int, val fbTerms: Int, val c: Double)

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "robust")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()
    val qrels = dataset.getQueryJudgments()
    val fbDocsN = argp.get("fbDocs", 10)
    val fbTermsN = argp.get("fbTerms", 100)
    val fbLambda = argp.get("fbLambda", 0.2)

    dataset.getIndex().use { retr ->
        val env = RREnv(retr)
        forEachQuery(dsName) { q ->
            if (q.qid != "301") return@forEachQuery
            val queryJudgments = qrels[q.qid]
            println("${q.qid} ${q.qterms}")

            val fbDocs = q.docs.take(fbDocsN)
            val terms = fbDocs.flatMapTo(HashSet<String>()) { it.terms }

            val fbTerms = HashMap<HyperParam, ArrayList<WeightedTerm>>()

            terms.forEach { term ->
                RRLogLogisticTFScore.OriginalPaperCValues.forEach { c ->
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

            fbTerms.mapValues { (params, wts) ->
                val expQ = env.sum(wts.sorted().take(params.fbTerms).map { RRDirichletTerm(env, it.term).weighted(it.score) })

                q.ranked()
            }


        }
    }
}
