package edu.umass.cics.ciir.dbpedia

import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluators

/**
 * @author jfoley
 */

fun main(args: Array<String>) {
    val dataset = DataPaths.DBPE
    val qrels = dataset.qrels
    val queries = dataset.title_qs.filterKeys { qrels.containsKey(it) }
    val evals = getEvaluators(listOf("ap", "ndcg", "p5"))

    println("${queries.size} ${qrels.size}")
    val ms = NamedMeasures()

    dataset.getIreneIndex().use { index ->
        queries.forEach { qid, qtext ->
            println("$qid $qtext")
            val queryJudgments = qrels[qid]!!
            val qterms = index.tokenize(qtext)

            val fieldExprs = listOf("body", "short", "citation_titles", "anchor_text", "redirects", "categories_text").map {
                MeanExpr(qterms.map { DirQLExpr(TextExpr(it)) })
            }
            val mixtureModel = CombineExpr(fieldExprs, listOf(0.3, 0.5, 0.05, 0.2, 0.1, 0.05))
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