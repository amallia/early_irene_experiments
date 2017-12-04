package edu.umass.cics.ciir.dbpedia

import edu.umass.cics.ciir.chai.normalize
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.toQueryResults
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluators
import edu.umass.cics.ciir.sprf.inqueryStop
import org.lemurproject.galago.utility.Parameters


/**
 *
 * @author jfoley.
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dataset = when(argp.get("dataset", "rewq")) {
        "rewq" -> DataPaths.REWQ_Clue12
        "dbpe" -> DataPaths.DBPE
        else -> error("Unknown dataset=${argp.getString("dataset")}")
    }
    val qrels = dataset.qrels
    val queries = dataset.title_qs.filterKeys { qrels.containsKey(it) }
    val evals = getEvaluators(listOf("ap", "ndcg", "p5"))

    println("${queries.size} ${qrels.size}")
    val ms = NamedMeasures()

    val fields = ArrayList<String>()
    if (argp.isList("fields") || argp.isString("fields")) {
        fields.addAll(argp.getAsList("fields", String::class.java))
    } else {
        fields.addAll(listOf("body", "short_text"))
        //fields.addAll(listOf<String>("body", "anchor_text", "short_text", "links", "props", "categories_text", "redirects", "citation_titles"))
    }
    val paramWeights = ArrayList<Double>()
    if (argp.isList("weights") || argp.isDouble("weights")) {
        val argW = argp.getAsList("weights", Any::class.java).map {
            when (it) {
                is Number -> it.toDouble()
                is String -> it.toDouble()
                else -> error("Can't make weight=$it into a double...")
            }
        }
        paramWeights.addAll(argW)
    }

    val model = argp.get("model", "avgl_fsdm")
    val avgDLMu = argp.get("avgDLMu", false)
    val defaultMu = argp.get("defaultDirichletMu", 7000.0)
    val depth = argp.get("depth", 100)
    val stopwords: Set<String> = if (argp.get("stopSDM", true)) { inqueryStop } else { emptySet() }

    //val fields = arrayListOf<String>("body", "anchor_text", "citation_titles", "redirects", "categories_text", "short_text")

    dataset.getIreneIndex().use { index ->
        index.env.estimateStats = argp.get("estimateStats", "min")
        val fieldMu = fields.associate { Pair(it, index.getAverageDL(it)) }
        println("fieldMu = $fieldMu")

        queries.forEach { qid, qtext ->
            println("$qid $qtext $model")
            val queryJudgments = qrels[qid]!!
            val qterms = index.tokenize(qtext)

            val query = when(model) {
                "mlm" -> CombineExpr(fields.map { field ->
                    MeanExpr(qterms.map { term ->
                        val mu = if (avgDLMu) fieldMu[field] else defaultMu
                        DirQLExpr(TextExpr(term, field), mu ?: defaultMu)
                    })
                }, paramWeights)
                "prms" -> MeanExpr(qterms.map { term ->
                    val weights: Map<String, Double> = fields.map { field ->
                        val stats = index.getStats(term, field)
                        Pair(field, stats.nonzeroCountProbability())
                    }.filterNotNull().associate { it }.normalize()

                    MeanExpr(weights.map { (field, weight) ->
                        val mu = if (avgDLMu) fieldMu[field] else defaultMu
                        WeightExpr(DirQLExpr(TextExpr(term, field), mu ?: defaultMu), weight)
                    })
                })
                "avgl_fsdm" -> SumExpr(fields.map { field ->
                    val weights = fieldMu.normalize()
                    val mu = if (avgDLMu) fieldMu[field] else defaultMu
                    //val weights = mapOf("body" to 0.7, "short_text" to 0.3)

                    // ap=0.231, ndcg=0.465, p5=0.595
                    // ap=0.234, ndcg=0.464, p5=0.595
                    // w/estimateStats = "min" // ap=0.233 ndcg=0.459, p5=0.605
                    // w/estimateStats = "prob" // ap=0.230 ndcg=0.459, p5=0.589
                    SequentialDependenceModel(qterms, field, statsField = "body", makeScorer = { DirQLExpr(it, mu ?: defaultMu) }, stopwords = stopwords).weighted(weights[field])
                })
                "sdm" -> CombineExpr(fields.map { field ->
                    val mu = if (avgDLMu) fieldMu[field] else defaultMu
                    SequentialDependenceModel(qterms, field, makeScorer = { DirQLExpr(it, mu ?: defaultMu) }, stopwords = stopwords, fullProx = 0.1);
                }, paramWeights)
                "mixture" -> {
                    val fieldExprs = listOf("body", "short_text").map { field ->
                        MeanExpr(qterms.map { DirQLExpr(TextExpr(it, field)) })
                    }
                    CombineExpr(fieldExprs, listOf(0.3, 0.5))
                }
                else -> error("No such model=$model")
            }
            println(query)
            val results = index.search(query, depth)
            val qres = results.toQueryResults(index)

            if (argp.get("printTopK", true)) {
                println(qres.take(5).joinToString(separator = "\n") { "${it.rank}\t${it.name}\t${it.score}" })
            }

            evals.forEach { measure, evalfn ->
                val score = try {
                    evalfn.evaluate(qres, queryJudgments)
                } catch (npe: NullPointerException) {
                    System.err.println("NULL in eval...")
                    -Double.MAX_VALUE
                }
                ms.push("$measure", score)
            }

            println(ms.means())
        }
    }

    println(ms.means())
}
