package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluators
import org.lemurproject.galago.utility.Parameters

/**
 * @author jfoley
 */

fun ReflexivePair(lhs: Int, rhs: Int) = Pair(minOf(lhs, rhs), maxOf(lhs, rhs))

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "robust")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()
    val qrels = dataset.getQueryJudgments()
    val fbTerms = 100
    val rmLambda = 0.2

    dataset.getIndex().use { retr ->
        val env = RREnv(retr)
        forEachQuery(dsName) { q ->
            val queryJudgments = qrels[q.qid]
            println(q.qterms)

            val similarities = HashMap<Pair<Int,Int>, Double>()
            val docVectors = q.docs.map { it.freqs }

            // All pairs:
            (0 until docVectors.size-1).forEach { i ->
                (i until docVectors.size).forEach { j ->
                    similarities.put(
                            ReflexivePair(i, j),
                            cosineSimilarity(docVectors[i], docVectors[j]))
                }
            }

            similarities.entries
        }
    }
}
fun cosineSimilarity(lhs: BagOfWords, rhs: BagOfWords): Double {
    var num = 0.0
    lhs.counts.forEachEntry { t, c ->
        val c2 = rhs.count(t)
        num += c * c2
        true
    }
    return num / (lhs.l2norm * rhs.l2norm)
}

