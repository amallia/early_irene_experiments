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
    val evals = getEvaluators(listOf("ap", "ndcg", "p5"))

    println("${queries.size} ${qrels.size}")
    val ms = NamedMeasures()

    val fields = arrayListOf<String>("body", "anchor_text", "short_text")
    //val fields = arrayListOf<String>("body", "anchor_text", "citation_titles", "redirects", "categories_text", "short_text")

    dataset.getIreneIndex().use { index ->
        val fieldMu = fields.associate { Pair(it, index.getAverageDL(it)*100.0) }
        println("fieldMu = $fieldMu")

        queries.forEach { qid, qtext ->
            val queryJudgments = qrels[qid]!!
            val qterms = index.tokenize(qtext)

            val prms = MeanExpr(qterms.map { term ->
                val weights: Map<String, Double> = fields.map { field ->
                    val stats = index.getStats(term, field)
                    if (stats == null) {
                        null
                    } else Pair(field, stats.nonzeroCountProbability())
                }.filterNotNull().associate {it}

                val norm = weights.values.sum()
                MeanExpr(weights.map { (field, weight) ->
                    WeightExpr(DirQLExpr(TextExpr(term, field), fieldMu[field]), weight / norm)
                })
            })
            println(prms)
            val fieldExprs = listOf("body", "short_text", "citation_titles", "anchor_text", "redirects", "categories_text").map { field ->
                MeanExpr(qterms.map { DirQLExpr(TextExpr(it, field)) })
            }
            val mixtureModel = CombineExpr(fieldExprs, listOf(0.3, 0.5, 0.05, 0.2, 0.1, 0.05))
            val results = index.search(prms, 100)
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
