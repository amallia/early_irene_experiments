package edu.umass.cics.ciir.iltr.rf

import edu.umass.cics.ciir.chai.computeStats
import edu.umass.cics.ciir.chai.smartLines
import edu.umass.cics.ciir.chai.smartPrint
import edu.umass.cics.ciir.iltr.LTRQuery
import edu.umass.cics.ciir.iltr.toRRExpr
import edu.umass.cics.ciir.irene.IIndex
import edu.umass.cics.ciir.irene.lang.FullDependenceModel
import edu.umass.cics.ciir.irene.lang.SequentialDependenceModel
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File
import kotlin.coroutines.experimental.buildSequence

/**
 * @author jfoley
 */
private fun forEachSDMPoolQuery(index: IIndex, fields: Set<String>, dsName: String, doFn: (LTRQuery) -> Unit) {
    readLTRQueries(index, fields, dsName).forEach(doFn)
}

fun readLTRQueries(index: IIndex, fields: Set<String>, dsName: String) = buildSequence {
    File("$dsName.irene-sdm.qlpool.jsonl.gz").smartLines { lines ->
        for (line in lines) {
            val qjson = Parameters.parseStringOrDie(line)
            val qid = qjson.getStr("qid")
            val qtext = qjson.getStr("qtext")
            val qterms = qjson.getAsList("qterms", String::class.java)

            val docs = qjson.getAsList("docs", Parameters::class.java).map { p ->
                val fjson = p.getMap("fields")
                LTRDoc.create(p.getStr("id"), fjson, fields, index)
            }

            yield(LTRQuery(qid, qtext, qterms, docs))
        }
    }
}


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