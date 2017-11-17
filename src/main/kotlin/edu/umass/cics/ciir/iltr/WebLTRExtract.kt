package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 * @author jfoley
 */
fun forEachSDMPoolQuery(tokenizer: GenericTokenizer, dsName: String, doFn: (LTRQuery) -> Unit) {
    File("$dsName.irene-sdm.qlpool.jsonl.gz").smartDoLines { line ->
        val qjson = Parameters.parseStringOrDie(line)
        val qid = qjson.getStr("qid")
        val qtext = qjson.getStr("qtext")
        val qterms = qjson.getAsList("qterms", String::class.java)

        val docs = qjson.getAsList("docs", Parameters::class.java).map { LTRDocOfWeb(tokenizer, it) }

        doFn(LTRQuery(qid, qtext, qterms, docs))
    }
}

object TrimJSONLDoc  {
}

val MandatoryFields = arrayListOf<String>("title", "body", "document")
fun LTRDocOfWeb(tokenizer: GenericTokenizer, p: Parameters): LTRDoc {
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
    features["title-ql-prior"] = p.getDouble("title-ql-prior")
    features["title-ql"] = p.getDouble("title-ql")

    return LTRDoc(name, features, rank, fields)
}

val WikiFields = setOf("body", "short_text", "title", "id")
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "mq07")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()
    val qrels = dataset.getQueryJudgments()
    val qid = argp.get("qid")?.toString()
    val qidBit = if (qid == null) "" else ".$qid"
    val statsField = argp.get("statsField", "document")
    val onlyEmitJudged = argp.get("onlyJudged", false)
    val judgedBit = if (onlyEmitJudged) ".judged" else ""

    File("l2rf/$dsName$judgedBit$qidBit.features.jsonl.gz").smartPrint { out ->
        Pair(WikiSource().getIreneIndex(), dataset.getIreneIndex()).use { wiki, index ->
            val env = index.getRREnv()
            env.estimateStats = "min"
            wiki.env.estimateStats = env.estimateStats
            wiki.env.defaultDirichletMu = 7000.0
            val wikiN = wiki.fieldStats("body")!!.dc.toDouble()

            val fieldWeights = listOf("body", "short_text").associate { it to wiki.fieldStats(it)!!.avgDL() }.normalize()

            forEachSDMPoolQuery(index.tokenizer, dsName) { q ->
                if (qid != null && qid != q.qid) {
                    // skip all but qid if specified.
                    return@forEachSDMPoolQuery
                }
                val queryJudgments = qrels[q.qid] ?: QueryJudgments(q.qid, emptyMap())
                val feature_exprs = HashMap<String, RRExpr>()
                val query_features = HashMap<String, Double>()

                val wikiQ =  SumExpr(
                        SequentialDependenceModel(q.qterms, field="short_text", statsField="body", stopwords=inqueryStop).weighted(fieldWeights["short_text"]),
                        SequentialDependenceModel(q.qterms, field="body", statsField="body", stopwords=inqueryStop).weighted(fieldWeights["body"]))
                val wikiTopDocs = wiki.search(wikiQ, 1000)
                val wikiScoreInfo = wikiTopDocs.scoreDocs.map { it.score }.asFeatureStats()
                val logSumExp = wikiTopDocs.logSumExp()

                // What percentage of wikipedia matches this query?
                query_features["wiki_hits"] = wikiTopDocs.totalHits.toDouble() / wikiN
                val topWikiLTRDocs = wikiTopDocs.scoreDocs.asSequence().mapIndexedNotNull { i, sdoc ->
                    val normScore = wikiScoreInfo.normalize(sdoc.score.toDouble())
                    val docFields = wiki.document(sdoc.doc, WikiFields)?.toParameters() ?: return@mapIndexedNotNull null

                    val fields = listOf(
                            LTRDocField("body", docFields.get("body", ""), wiki.tokenizer),
                            LTRDocField("title", docFields.get("title", ""), wiki.tokenizer),
                            LTRDocField("short_text", docFields.get("short_text", ""), wiki.tokenizer)
                    ).associateTo(HashMap()) { it.toEntry() }
                    val features = hashMapOf(
                            "title-ql" to sdoc.score.toDouble(),
                            "title-ql-prior" to Math.exp(sdoc.score.toDouble() - logSumExp),
                            "wiki-score-norm" to normScore
                    )
                    LTRDoc(docFields.getStr("id"), features, i+1, fields, "body")
                }.take(50).toList()


                val sourceFields = arrayListOf("short_text", "body")
                val relevanceModelDepths = arrayListOf(5,10,25)
                val targetFields = arrayListOf("title", "body", "document")
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
                                    val rmeExpr = rm.toQExpr(fbTerms, targetField = tf, statsField = statsField).toRRExpr(env)
                                    feature_exprs.put(key, rmeExpr)
                                }
                            }
                        }
                    }
                }

                arrayListOf("title", "body", "document").forEach { fieldName ->
                    val qterms = index.tokenize(q.qtext, fieldName)
                    println("${q.qid} $fieldName: $qterms")
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
                    val rm = computeRelevanceModel(q.docs, "title-ql-prior", fbDocs, field=env.defaultField)
                    arrayListOf("title", "body", "document").forEach { fieldName ->
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
                    arrayListOf<String>("norm:body:wiki.short_text-rm-k10-t100", "norm:body:rm-k10-t100").forEach { method ->
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
}
