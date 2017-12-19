package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.irene.IIndex
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.scoring.ILTRDocField
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import edu.umass.cics.ciir.irene.scoring.LTRDocField
import edu.umass.cics.ciir.irene.scoring.LTREmptyDocField
import edu.umass.cics.ciir.irene.toParameters
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 * @author jfoley
 */
private fun forEachSDMPoolQuery(index: IIndex, fields: Set<String>, dsName: String, doFn: (LTRQuery) -> Unit) {
    File("$dsName.irene.pool.jsonl.gz").smartDoLines { line ->
        val qjson = Parameters.parseStringOrDie(line)
        val qid = qjson.getStr("qid")
        val qtext = qjson.getStr("qtext")
        val qterms = qjson.getAsList("qterms", String::class.java)

        val docs = qjson.getAsList("docs", Parameters::class.java).map { LTRDocFromIndex(index, fields, it) }

        doFn(LTRQuery(qid, qtext, qterms, docs))
    }
}

fun LTRDocFromIndex(index: IIndex, mandatoryFields: Set<String>, p: Parameters): LTRDoc {
    val fjson = p.getMap("fields")

    val features = HashMap<String, Double>()
    val fields = HashMap<String, ILTRDocField>()

    fjson.keys.forEach { key ->
        if (fjson.isString(key)) {
            val fieldText = fjson.getStr(key)
            fields.put(key, LTRDocField(key, fieldText, index.tokenizer))
        } else if(fjson.isDouble(key)) {
            features.put("double-field-$key", fjson.getDouble(key))
        } else if(fjson.isLong(key)) {
            features.put("long-field-$key", fjson.getLong(key).toDouble())
        } else {
            println("Warning: Can't handle field: $key=${fjson[key]}")
        }
    }

    mandatoryFields
            .filterNot { fields.containsKey(it) }
            .forEach { fields[it] = LTREmptyDocField(it) }

    val name = p.getStr("id")
    return LTRDoc(name, features, -1, fields)
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "nyt-cite")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()
    val qrels = dataset.getQueryJudgments()
    val qid = argp.get("qid")?.toString()
    val qidBit = if (qid == null) "" else ".$qid"
    val onlyEmitJudged = argp.get("onlyJudged", false)
    val judgedBit = if (onlyEmitJudged) ".judged" else ""
    val outDir = argp.get("output-dir", "nyt-cite-ltr/")

    val wikiSource = WikiSource()
    val wiki = wikiSource.getIreneIndex()
    val WikiFields = wikiSource.textFields + setOf(wiki.idFieldName, wiki.defaultField)
    val fieldWeights = wikiSource.textFields.associate { it to wiki.fieldStats(it)!!.avgDL() }.normalize()

    File("$outDir/$dsName$judgedBit$qidBit.features.jsonl.gz").smartPrint { out ->
        dataset.getIreneIndex().use { index ->
            val env = index.getRREnv()
            env.estimateStats = "min"
            wiki.env.estimateStats = env.estimateStats
            wiki.env.defaultDirichletMu = 7000.0
            val wikiN = wiki.fieldStats("body")!!.dc.toDouble()

            forEachSDMPoolQuery(index, dataset.textFields, dsName) { q ->
                if (qid != null && qid != q.qid) {
                    // skip all but qid if specified.
                    return@forEachSDMPoolQuery
                }
                val queryJudgments = qrels[q.qid] ?: QueryJudgments(q.qid, emptyMap())
                val feature_exprs = HashMap<String, RRExpr>()
                val query_features = HashMap<String, Double>()

                val qdf = StreamingStats().apply {
                    q.qterms.forEach { push(env.getStats(it, dataset.statsField).binaryProbability()) }
                }
                query_features["q_min_df"] = qdf.min
                query_features["q_mean_df"] = qdf.mean
                query_features["q_max_df"] = qdf.max
                query_features["q_stddev_df"] = qdf.standardDeviation

                val wdf = StreamingStats().apply {
                    q.qterms.forEach { push(wiki.env.getStats(it, "body").binaryProbability()) }
                }
                query_features["q_wiki_min_df"] = wdf.min
                query_features["q_wiki_mean_df"] = wdf.mean
                query_features["q_wiki_max_df"] = wdf.max
                query_features["q_wiki_stddev_df"] = wdf.standardDeviation

                // TODO, move this into WikiSource somehow.
                val wikiQ = SumExpr(
                        SequentialDependenceModel(q.qterms, field = "short_text", statsField = "body", stopwords = inqueryStop).weighted(fieldWeights["short_text"]),
                        SequentialDependenceModel(q.qterms, field = "body", statsField = "body", stopwords = inqueryStop).weighted(fieldWeights["body"]))
                val wikiTopDocs = wiki.search(wikiQ, 1000)
                val wikiScoreInfo = wikiTopDocs.scoreDocs.map { it.score }.computeStats()

                // What percentage of wikipedia matches this query?
                query_features["wiki_hits"] = wikiTopDocs.totalHits.toDouble() / wikiN
                val topWikiLTRDocs = wikiTopDocs.scoreDocs.asSequence().mapIndexedNotNull { i, sdoc ->
                    val normScore = wikiScoreInfo.maxMinNormalize(sdoc.score.toDouble())
                    val docFields = wiki.document(sdoc.doc, WikiFields)?.toParameters() ?: return@mapIndexedNotNull null

                    val fields = wikiSource.textFields.map { fname ->
                        val text = docFields.get(fname, "")
                        LTRDocField(fname, text, wiki.tokenizer)
                    }.associateTo(HashMap()) { it.toEntry() }

                    val features = hashMapOf( "wiki-score-norm" to normScore)
                    LTRDoc(docFields.getStr("id"), features, i+1, fields, wiki.defaultField)
                }.take(50).toList()

                val sourceFields = wikiSource.textFields.toList()
                val relevanceModelDepths = arrayListOf(5,10,25)
                val targetFields = dataset.textFields
                val numRMTerms = arrayListOf(10,50,100)

                relevanceModelDepths.forEach { k ->
                    query_features["wiki-score-norm-mean-k$k"] = topWikiLTRDocs.take(k).map { it.features["wiki-score-norm"]!! }.mean()
                }

                sourceFields.forEach { sourceField ->
                    relevanceModelDepths.forEach { fbDocs ->
                        val rm = computeRelevanceModel(topWikiLTRDocs, "wiki-score-norm", fbDocs, sourceField)
                        if (!rm.weights.isEmpty) {
                            targetFields.forEach { tf ->
                                numRMTerms.forEach { fbTerms ->
                                    val key = "norm:$tf:wiki.$sourceField-rm-k$fbDocs-t$fbTerms"
                                    val rmeExpr = rm.toQExpr(fbTerms, targetField = tf, statsField = dataset.statsField).toRRExpr(env)
                                    feature_exprs.put(key, rmeExpr)
                                }
                            }
                        }
                    }
                }

                dataset.textFields.forEach { fieldName ->
                    val qterms = index.tokenize(q.qtext, fieldName)
                    println("${q.qid} $fieldName: $qterms")
                    q.docs.forEach { doc ->
                        doc.features["$fieldName:qlen"] = qterms.size.toDouble()
                        doc.features["$fieldName:qstop"] = qterms.count { inqueryStop.contains(it) }.toDouble()
                    }

                    // Retrieval models.
                    feature_exprs.putAll(hashMapOf<String, QExpr>(
                            Pair("norm:$fieldName:bm25", UnigramRetrievalModel(qterms, { BM25Expr(it) }, fieldName, dataset.statsField)),
                            Pair("norm:$fieldName:LM-dir", QueryLikelihood(qterms, fieldName, dataset.statsField)),
                            Pair("norm:$fieldName:fdm-stop", FullDependenceModel(qterms, field = fieldName, statsField = dataset.statsField, stopwords = inqueryStop)),
                            Pair("norm:$fieldName:sdm-stop", SequentialDependenceModel(qterms, field = fieldName, statsField = dataset.statsField, stopwords = inqueryStop))
                    ).mapValues { (_,q) -> q.toRRExpr(env) })

                    feature_exprs.putAll(hashMapOf<String, RRExpr>(
                            Pair("docinfo", RRDocInfoQuotient(env, fieldName)),
                            Pair("avgwl", RRAvgWordLength(env, field = fieldName)),
                            Pair("meantp", env.mean(qterms.map { RRTermPosition(env, it, fieldName) })),
                            Pair("jaccard-stop", RRJaccardSimilarity(env, inqueryStop, field = fieldName)),
                            Pair("entropy", RREntropy(env, field = fieldName)),
                            Pair("length", RRDocLength(env, field = fieldName))
                    ).mapKeys { (k, _) -> "$fieldName:$k" })
                }

                val docList = if (onlyEmitJudged) {
                    q.docs.filter { queryJudgments.isJudged(it.name) }
                } else {
                    q.docs
                }

                docList.forEach { doc ->
                    doc.features.putAll(query_features)
                }

                val skippedFeatures = ConcurrentHashMap<String, Int>()
                docList.forEach { doc ->
                    doc.evalAndSetFeatures(feature_exprs, skippedFeatures)
                }
                if (skippedFeatures.isNotEmpty()) {
                    println("Skipped NaN or Infinite features: ${skippedFeatures}")
                    skippedFeatures.clear()
                }

                val feature_exprs_2pass = HashMap<String, RRExpr>()
                arrayListOf<Int>(5, 10, 25).forEach { fbDocs ->
                    val rm = computeRelevanceModel(q.docs, "norm:${env.defaultField}:sdm-stop", fbDocs, field=env.defaultField, logSumExp = true)
                    dataset.textFields.forEach { fieldName ->
                        numRMTerms.forEach { fbTerms ->
                            val wt = rm.toTerms(fbTerms)
                            val rmeExpr = rm.toQExpr(fbTerms, targetField = fieldName, statsField=dataset.statsField).toRRExpr(env)
                            feature_exprs_2pass.put("norm:$fieldName:rm-k$fbDocs-t$fbTerms", rmeExpr)
                            feature_exprs_2pass.put("$fieldName:jaccard-rm-k$fbDocs-t$fbTerms", RRJaccardSimilarity(env, wt.map { it.term }.toSet(), field = fieldName))
                        }
                    }
                }

                docList.forEach { doc ->
                    doc.evalAndSetFeatures(feature_exprs_2pass, skippedFeatures)
                }
                if (skippedFeatures.isNotEmpty()) {
                    println("Skipped NaN or Infinite features: ${skippedFeatures}")
                    skippedFeatures.clear()
                }

                if (!onlyEmitJudged) {
                    arrayListOf<String>("norm:${index.defaultField}:wiki.short_text-rm-k10-t100", "norm:${index.defaultField}:rm-k10-t100").forEach { method ->
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
                println(ms.means())
            }
        }
    }
    wiki.close()
}
