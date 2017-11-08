package edu.umass.cics.ciir.dbpedia

import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluators
import org.lemurproject.galago.utility.Parameters

inline fun <T> List<T>.forEachSeqPair(fn: (T,T)->Unit) {
    (0 until this.size-1).forEach { i ->
        fn(this[i], this[i+1])
    }
}

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
        fields.addAll(listOf("body", "anchor_text"))
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
    } else {
        paramWeights.addAll(fields.map { 1.0 })
    }

    val model = argp.get("model", "ubg")
    val avgDLMu = argp.get("avgDLMu", false)
    val defaultMu = argp.get("mu", 7000.0)
    val depth = argp.get("depth", 100)
    val bgW = argp.get("bgW", 0.7)
    val ugW = argp.get("ugW", 0.3)

    //val fields = arrayListOf<String>("body", "anchor_text", "citation_titles", "redirects", "categories_text", "short_text")

    dataset.getIreneIndex().use { index ->
        val fieldMu = fields.associate { Pair(it, index.getAverageDL(it)) }
        println("fieldMu = $fieldMu")

        queries.forEach { qid, qtext ->
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
                        if (stats == null) {
                            null
                        } else Pair(field, stats.nonzeroCountProbability())
                    }.filterNotNull().associate {it}

                    val norm = weights.values.sum()
                    MeanExpr(weights.map { (field, weight) ->
                        val mu = if (avgDLMu) fieldMu[field] else defaultMu
                        WeightExpr(DirQLExpr(TextExpr(term, field), mu ?: defaultMu), weight / norm)
                    })
                })
                "ubg" -> CombineExpr(fields.map { field ->
                    val mu = if (avgDLMu) fieldMu[field] else defaultMu
                    val unigrams = MeanExpr(qterms.map { term ->
                        DirQLExpr(TextExpr(term, field), mu ?: defaultMu)
                    })
                    val bigrams = ArrayList<QExpr>()
                    qterms.forEachSeqPair { t1, t2 ->
                        val q = DirQLExpr(OrderedWindowExpr(listOf(TextExpr(t1, field), TextExpr(t2, field))), mu ?: defaultMu)
                        bigrams.add(q)
                    }
                    if (bigrams.isNotEmpty()) {
                        SumExpr(MeanExpr(bigrams).weighted(bgW), unigrams.weighted(ugW))
                    } else {
                        unigrams
                    }
                }, paramWeights)
                "mixture" -> {
                    val fieldExprs = listOf("body", "short_text").map { field ->
                        MeanExpr(qterms.map { DirQLExpr(TextExpr(it, field)) })
                    }
                    CombineExpr(fieldExprs, listOf(0.3, 0.5))
                }
                else -> error("No such model=$model")
            }
            val results = index.search(query, depth)
            val qres = results.toQueryResults(index)

            if (argp.get("printTopK", false)) {
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
