package edu.umass.cics.ciir.iltr.rf

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.iltr.*
import edu.umass.cics.ciir.irene.IIndex
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import edu.umass.cics.ciir.irene.scoring.LTRDocField
import edu.umass.cics.ciir.irene.toParameters
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.experimental.buildSequence

/**
 * @author jfoley
 */
private fun forEachSDMPoolQuery(index: IIndex, fields: Set<String>, dsName: String, doFn: (LTRQuery) -> Unit) {
    readLTRQueries(index, fields, dsName).forEach(doFn)
}

fun readLTRQueries(input: File, fields: Set<String>, index: IIndex): Sequence<LTRQuery> = buildSequence {
    input.smartLines { lines ->
        for (line in lines) {
            val qjson = Parameters.parseStringOrDie(line)
            val qid = qjson.getStr("qid")
            val qtext = qjson.getStr("qtext")
            val qterms = qjson.getAsList("qterms", String::class.java)

            val docs = qjson.getAsList("docs", Parameters::class.java).map { p ->
                val fjson = p.getMap("fields")
                val ltrDoc = LTRDoc.create(p.getStr("id"), fjson, fields, index)
                val fMap = p.getMap("features")
                fMap.keys.forEach { k ->
                    ltrDoc.features.put(k, fMap.getDouble(k))
                }
                ltrDoc
            }

            yield(LTRQuery(qid, qtext, qterms, docs))
        }
    }
}

fun readLTRQueries(index: IIndex, fields: Set<String>, dsName: String) = readLTRQueries(File("$dsName.irene-sdm.qlpool.jsonl.gz"), fields, index)


fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "mq07")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val map = getEvaluator("ap")
    val ms = NamedMeasures()
    val qrels = dataset.getQueryJudgments()
    val outDir = argp.get("output-dir", "passage-iltr")
    val passageSize = argp.get("passageSize", 100)
    val passageStep = argp.get("passageStep", 50)
    val passageRankingFeatureName = "norm:passage:fdm"

    File("$outDir/$dsName.passages.jsonl.gz").smartPrint { out ->
        dataset.getIreneIndex().use { index ->
            val env = index.getRREnv()
            env.estimateStats = "min"
            forEachSDMPoolQuery(index, dataset.textFields, dsName) { q ->
                val judgments = qrels[q.qid] ?: QueryJudgments(q.qid, emptyMap())
                val N = q.docs.size

                // Create FDM and lookup stats to score passages.
                val fdm= FullDependenceModel(q.qterms, stopwords= inqueryStop).toRRExpr(index.env)
                val sdm= SequentialDependenceModel(q.qterms, stopwords= inqueryStop).toRRExpr(index.env)

                val maxPassageList = q.docs.mapNotNull { doc ->
                    val terms = doc.target().terms
                    val docFDM = fdm.eval(doc)
                    val docSDM = sdm.eval(doc)
                    val passages = terms.windowed(passageSize, passageStep, partialWindows = true).asSequence().map { pterms ->
                        val pdoc = doc.forPassage(doc.defaultField, pterms)
                        pdoc.features[passageRankingFeatureName] = fdm.eval(pdoc)
                        pdoc.features["norm:doc:fdm"] = docFDM
                        pdoc.features["norm:passage:sdm"] = sdm.eval(pdoc)
                        pdoc.features["norm:doc:sdm"] = docSDM
                        pdoc
                    }

                    val pstats = passages.mapNotNull { it.features["norm:passage:fdm"] }.computeStats()

                    val mp = passages.maxBy { it.features["norm:passage:fdm"] ?: Double.NEGATIVE_INFINITY }
                    if (mp != null) {
                        mp.features.putAll(pstats.features.mapKeys { (k,_) -> "norm:passages:fdm:$k" })
                        for (m in listOf("mean", "max", "min")) {
                            mp.features.put("mix", pstats.mean + pstats.max + pstats.min)
                        }
                    }
                    mp
                }

                // Doc vs. Passage Scoring:
                val pq = LTRQuery(q.qid, q.qtext, q.qterms, maxPassageList)
                ms.push("MP-FDM-AP", map.evaluate(pq.toQResults(passageRankingFeatureName), judgments))
                ms.push("MD-FDM-AP", map.evaluate(pq.toQResults("norm:doc:fdm"), judgments))
                ms.push("MP-SDM-AP", map.evaluate(pq.toQResults("norm:passage:sdm"), judgments))
                ms.push("MD-SDM-AP", map.evaluate(pq.toQResults("norm:doc:sdm"), judgments))
                for (m in listOf("mean", "max", "min", "mix")) {
                    ms.push("P-$m-FDM-AP", map.evaluate(pq.toQResults("norm:passages:fdm:$m"), judgments))
                }

                println("${q.qid} $ms")

                out.println(pq.toJSONDocs())
            }
        }
    }
    println(ms)
}

object PassageLTRExtract {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dsName = argp.get("dataset", "nyt-cite")
        val dataset = DataPaths.get(dsName)
        val evals = getEvaluators(listOf("ap", "ndcg"))
        val ms = NamedMeasures()
        val qrels = dataset.getQueryJudgments()
        val outDir = File(argp.get("output-dir", "passage-iltr/"))

        val wikiSource = WikiSource()
        val wiki = wikiSource.getIreneIndex()
        val WikiFields = wikiSource.textFields + setOf(wiki.idFieldName, wiki.defaultField)
        val fieldWeights = wikiSource.textFields.associate { it to wiki.fieldStats(it)!!.avgDL() }.normalize()

        val msg = CountingDebouncer(total=qrels.size.toLong())

        File(outDir, "$dsName.features.jsonl.gz").smartPrint { out ->
            dataset.getIreneIndex().use { index ->
                val env = index.getRREnv()
                env.estimateStats = "min"
                wiki.env.estimateStats = env.estimateStats
                wiki.env.defaultDirichletMu = 7000.0
                val wikiN = wiki.fieldStats("body")!!.dc.toDouble()
                for (q in readLTRQueries(File(outDir, "$dsName.passages.jsonl.gz"), setOf(index.defaultField), index)) {
                    println("${q.qid} ${q.qterms}")

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
                    val topWikiLTRDocs = wikiTopDocs.scoreDocs.asSequence().mapNotNull { sdoc ->
                        val normScore = wikiScoreInfo.maxMinNormalize(sdoc.score.toDouble())
                        val docFields = wiki.document(sdoc.doc, WikiFields)?.toParameters() ?: return@mapNotNull null

                        val fields = wikiSource.textFields.map { fname ->
                            val text = docFields.get(fname, "")
                            LTRDocField(fname, text, wiki.tokenizer)
                        }.associateTo(HashMap()) { it.toEntry() }

                        val features = hashMapOf("wiki-score-norm" to normScore)
                        LTRDoc(docFields.getStr("id"), features, fields, wiki.defaultField)
                    }.take(50).toList()

                    val sourceFields = wikiSource.textFields.toList()
                    val relevanceModelDepths = arrayListOf(5, 10, 25)
                    val numRMTerms = arrayListOf(10, 50, 100)

                    relevanceModelDepths.forEach { k ->
                        query_features["wiki-score-norm-mean-k$k"] = topWikiLTRDocs.take(k).map { it.features["wiki-score-norm"]!! }.mean()
                    }

                    sourceFields.forEach { sourceField ->
                        relevanceModelDepths.forEach { fbDocs ->
                            val rm = computeRelevanceModel(topWikiLTRDocs, "wiki-score-norm", fbDocs, sourceField)
                            if (!rm.weights.isEmpty) {
                                numRMTerms.forEach { fbTerms ->
                                    val key = "norm:wiki.$sourceField-rm-k$fbDocs-t$fbTerms"
                                    val rmeExpr = rm.toQExpr(fbTerms, targetField = dataset.statsField).toRRExpr(env)
                                    feature_exprs.put(key, rmeExpr)
                                }
                            }
                        }
                    }

                    val qterms = q.qterms
                    q.docs.forEach { doc ->
                        doc.features["qlen"] = qterms.size.toDouble()
                        doc.features["qstop"] = qterms.count { inqueryStop.contains(it) }.toDouble()
                    }

                    // Retrieval models.
                    feature_exprs.putAll(hashMapOf<String, QExpr>(
                            Pair("norm:bm25", UnigramRetrievalModel(qterms, { BM25Expr(it) }, dataset.statsField)),
                            Pair("norm:LM-dir", QueryLikelihood(qterms, dataset.statsField)),
                            Pair("norm:fdm-stop", FullDependenceModel(qterms, statsField = dataset.statsField, stopwords = inqueryStop)),
                            Pair("norm:sdm-stop", SequentialDependenceModel(qterms, statsField = dataset.statsField, stopwords = inqueryStop))
                    ).mapValues { (_, q) -> q.toRRExpr(env) })

                    feature_exprs.putAll(mapOf<String, RRExpr>(
                            Pair("docinfo", RRDocInfoQuotient(env)),
                            Pair("avgwl", RRAvgWordLength(env, index.defaultField)),
                            Pair("meantp", env.mean(qterms.map { RRTermPosition(env, it) })),
                            Pair("jaccard-stop", RRJaccardSimilarity(env, inqueryStop)),
                            Pair("entropy", RREntropy(env)),
                            Pair("length", RRDocLength(env))
                    ))

                    val docList = q.docs

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
                        val rm = computeRelevanceModel(q.docs, "norm:sdm-stop", fbDocs, field = env.defaultField, logSumExp = true)
                        numRMTerms.forEach { fbTerms ->
                            val wt = rm.toTerms(fbTerms)
                            val rmeExpr = rm.toQExpr(fbTerms, statsField = dataset.statsField).toRRExpr(env)
                            feature_exprs_2pass.put("norm:rm-k$fbDocs-t$fbTerms", rmeExpr)
                            feature_exprs_2pass.put("jaccard-rm-k$fbDocs-t$fbTerms", RRJaccardSimilarity(env, wt.map { it.term }.toSet()))
                        }
                    }

                    docList.forEach { doc ->
                        doc.evalAndSetFeatures(feature_exprs_2pass, skippedFeatures)
                    }
                    if (skippedFeatures.isNotEmpty()) {
                        println("Skipped NaN or Infinite features: ${skippedFeatures}")
                        skippedFeatures.clear()
                    }

                    arrayListOf<String>("norm:wiki.short_text-rm-k10-t100", "norm:rm-k10-t100").forEach { method ->
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

                    docList.forEach {
                        out.println(it.toJSONFeatures(queryJudgments, q.qid))
                    }
                    println(ms.means())

                    msg.incr()?.let { upd ->
                        println("mix=${docList[0].features["mix"]} $upd")
                    }
                }
            }
        }
        // Done!
    }
}