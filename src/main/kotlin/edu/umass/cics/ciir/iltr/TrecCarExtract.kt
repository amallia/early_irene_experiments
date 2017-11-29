package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.chai.CountingDebouncer
import edu.umass.cics.ciir.chai.StreamingStats
import edu.umass.cics.ciir.chai.smartDoLines
import edu.umass.cics.ciir.chai.smartPrint
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 * @author jfoley
 */
private fun forEachSDMPoolQuery(tokenizer: GenericTokenizer, input: File, doFn: (LTRQuery) -> Unit) {
    input.smartDoLines { line ->
        try {
            val qjson = Parameters.parseString(line)
            val qid = qjson.getStr("qid")
            val qtext = qjson.getStr("qtext")
            val qterms = qjson.getAsList("qterms", String::class.java)

            val docs = qjson.getAsList("docs", Parameters::class.java).map { LTRDocOfCAR(tokenizer, it) }

            doFn(LTRQuery(qid, qtext, qterms, docs))
        } catch (e: Exception) {
            // Error, probably parsing json..
            println("forEachQuery: $e")
        }
    }
}

val CARFields = arrayListOf<String>("text", "links")
fun LTRDocOfCAR(tokenizer: GenericTokenizer, p: Parameters): LTRDoc {
    val fjson = p.getMap("fields")

    val features = HashMap<String, Double>()
    val fields = HashMap<String, ILTRDocField>()

    fjson.keys.forEach { key ->
        if (fjson.isString(key)) {
            val fieldText = fjson.getStr(key)
            fields.put(key, LTRDocField(key, fieldText, tokenizer))
        } else if(fjson.isDouble(key)) {
            features.put("double-field-$key", fjson.getDouble(key))
        } else if(fjson.isLong(key)) {
            features.put("long-field-$key", fjson.getLong(key).toDouble())
        } else {
            println("Warning: Can't handle field: $key=${fjson[key]}")
        }
    }

    MandatoryFields
            .filterNot { fields.containsKey(it) }
            .forEach { fields[it] = LTREmptyDocField(it) }

    val name = p.getStr("id")
    val rank = p.getInt("rank")
    features["norm:pooling-score"] = p.getDouble("pooling-score")

    return LTRDoc(name, features, rank, fields)
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "trec-car-test200")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()
    //val qrels = dataset.getQueryJudgments()
    val qrels = QuerySetJudgments("data/merged.qrels.gz")
    val qid = argp.get("qid")?.toString()
    val statsField = argp.get("statsField", "text")
    val onlyEmitJudged = argp.get("onlyJudged", false)
    val outDir = argp.get("output-dir", "l2rf/latest/")

    val inputF = File("trec-car-100k.jsonl.gz")
    //val qidBit = if (qid == null) "" else ".$qid"
    //val judgedBit = if (onlyEmitJudged) ".judged" else ""
    //val outputF = File("$outDir/$dsName$judgedBit$qidBit.features.jsonl.gz")
    val outputF = File("$outDir/trec-car-100k.features.jsonl.gz")

    outputF.smartPrint { out ->
        dataset.getIreneIndex().use { index ->
            val env = index.getRREnv()
            env.estimateStats = "min"

            val msg = CountingDebouncer(Math.min(10000,qrels.size).toLong())
            forEachSDMPoolQuery(LuceneTokenizer(IreneEnglishAnalyzer()), inputF) { q ->
                if (qid != null && qid != q.qid) {
                    // skip all but qid if specified.
                    return@forEachSDMPoolQuery
                }
                val queryJudgments = qrels[q.qid] ?: QueryJudgments(q.qid, emptyMap())

                val numJudged = q.docs.count { queryJudgments.isJudged(it.name) }
                if (numJudged == 0) {
                    //println("Skip -- no judged in pool.")
                    return@forEachSDMPoolQuery
                }

                val feature_exprs = HashMap<String, RRExpr>()
                val query_features = HashMap<String, Double>()

                val qdf = StreamingStats().apply {
                    q.qterms.forEach { push(env.getStats(it, statsField).binaryProbability()) }
                }
                query_features["q_min_df"] = qdf.min
                query_features["q_mean_df"] = qdf.mean
                query_features["q_max_df"] = qdf.max
                query_features["q_stddev_df"] = qdf.standardDeviation

                val numRMTerms = arrayListOf(10,50,100)

                CARFields.forEach { fieldName ->
                    val qterms = index.tokenize(q.qtext, fieldName)
                    //println("\t$fieldName: $qterms")
                    q.docs.forEach { doc ->
                        doc.features["$fieldName:qlen"] = qterms.size.toDouble()
                        doc.features["$fieldName:qstop"] = qterms.count { inqueryStop.contains(it) }.toDouble()
                    }

                    // Retrieval models.
                    feature_exprs.putAll(hashMapOf<String, QExpr>(
                            Pair("norm:$fieldName:bm25", UnigramRetrievalModel(qterms, {BM25Expr(it)}, fieldName, statsField)),
                            Pair("norm:$fieldName:LM-dir", QueryLikelihood(qterms, fieldName, statsField)),
                            Pair("norm:$fieldName:LM-abs", UnigramRetrievalModel(qterms, {AbsoluteDiscountingQLExpr(it)}, fieldName, statsField)),
                            Pair("norm:$fieldName:fdm-stop", FullDependenceModel(qterms, field = fieldName, statsField=statsField, stopwords = inqueryStop)),
                            Pair("norm:$fieldName:sdm-stop", SequentialDependenceModel(qterms, field = fieldName, statsField=statsField, stopwords = inqueryStop))
                    ).mapValues { (_,q) -> q.toRRExpr(env) })

                    feature_exprs.putAll(hashMapOf<String, RRExpr>(
                            Pair("docinfo", RRDocInfoQuotient(env, fieldName)),
                            Pair("avgwl", RRAvgWordLength(env, field = fieldName)),
                            Pair("meantp", env.mean(qterms.map { RRTermPosition(env, it, fieldName) })),
                            Pair("jaccard-stop", RRJaccardSimilarity(env, inqueryStop, field = fieldName)),
                            Pair("length", RRDocLength(env, field = fieldName))).mapKeys { (k, _) -> "$fieldName:$k" }
                    )
                }

                arrayListOf<Int>(5, 10, 25).forEach { fbDocs ->
                    val rm = computeRelevanceModel(q.docs, "norm:pooling-score", fbDocs, field=env.defaultField, logSumExp = true)
                    CARFields.forEach { fieldName ->
                        numRMTerms.forEach { fbTerms ->
                            val wt = rm.toTerms(fbTerms)
                            val rmeExpr = rm.toQExpr(fbTerms, targetField = fieldName, statsField=statsField).toRRExpr(env)
                            feature_exprs.put("norm:$fieldName:rm-k$fbDocs-t$fbTerms", rmeExpr)
                            feature_exprs.put("$fieldName:jaccard-rm-k$fbDocs-t$fbTerms", RRJaccardSimilarity(env, wt.map { it.term }.toSet(), field = fieldName))
                        }
                    }
                }

                val docList = if (onlyEmitJudged) {
                    q.docs.filter { queryJudgments.isJudged(it.name) }
                } else {
                    q.docs
                }

                val skippedFeatures = ConcurrentHashMap<String, Int>()
                docList.parallelStream().forEach { doc ->
                    feature_exprs.forEach { fname, fexpr ->
                        val value = fexpr.eval(doc)
                        if (value.isInfinite() || value.isNaN()) {
                            skippedFeatures.incr(fname, 1)
                        } else {
                            doc.features.put(fname, value)
                        }
                    }
                }

                if (skippedFeatures.isNotEmpty()) {
                    println("Skipped NaN or Infinite features: ${skippedFeatures}")
                }
                docList.forEach { doc ->
                    doc.features.putAll(query_features)
                }

                if (!onlyEmitJudged) {
                    arrayListOf<String>("norm:text:bm25", "norm:text:rm-k10-t100").forEach { method ->
                        evals.forEach { measure, evalfn ->
                            val score = try {
                                val results = q.toQResults(method)
                                if (results.isEmpty()) {
                                    0.0
                                } else {
                                    evalfn.evaluate(results, queryJudgments)
                                }
                            } catch (npe: NullPointerException) {
                                System.err.println("NULL in eval...")
                                npe.printStackTrace()
                                -Double.MAX_VALUE
                            }
                            ms.push("$measure.$method", score)
                        }
                    }
                }

                docList.forEach {
                    out.println(it.toJSONFeatures(queryJudgments, q.qid))
                }
                msg.incr()?.let { upd ->
                    println(ms)
                    println(upd)
                }
            }

            println("DONE.")
            println(ms)
        }
    }
}
