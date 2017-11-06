package edu.umass.cics.ciir.dbpedia

import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluators

/**
 *
 * @author jfoley.
 */
fun main(args: Array<String>) {
    val dataset = DataPaths.REWQ_Clue12
    val qrels = dataset.qrels
    val queries = dataset.title_qs.filterKeys { qrels.containsKey(it) }
    val evals = getEvaluators(listOf("ap", "ndcg"))

    println("${queries.size} ${qrels.size}")
    val ms = NamedMeasures()

    dataset.getIreneIndex().use { index ->
        queries.forEach { qid, qtext ->
            val queryJudgments = qrels[qid]!!
            val qterms = index.tokenize(qtext)

            val fieldExprs = listOf("body", "short").map {
                MeanExpr(qterms.map { DirQLExpr(TextExpr(it)) })
            }
            val mixtureModel = CombineExpr(fieldExprs, listOf(0.4, 0.6))
            val results = index.search(mixtureModel, 100)
            val qres = results.toQueryResults(index)

            println(qres.take(5).joinToString(separator="\n") { "${it.rank}\t${it.name}\t${it.score}" })

            evals.forEach { measure, evalfn ->
                val score = try {
                    evalfn.evaluate(qres, queryJudgments)
                } catch (npe: NullPointerException) {
                    System.err.println("NULL in eval...")
                    -Double.MAX_VALUE
                }
                ms.push("$measure.irene2", score)
            }

            println(ms.means())
        }
    }

    println(ms.means())
}