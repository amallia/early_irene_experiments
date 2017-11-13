package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.chai.smartPrint
import edu.umass.cics.ciir.irene.SequentialDependenceModel
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.SimpleEvalDoc
import org.lemurproject.galago.utility.MathUtils
import org.lemurproject.galago.utility.Parameters
import java.io.File

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "robust")
    val dataset = DataPaths.get(dsName)
    val qrels = dataset.getQueryJudgments()
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()
    val depth = argp.get("depth", 1000)

    File("$dsName.irene-sdm.qlpool.jsonl.gz").smartPrint { output ->
        dataset.getIreneIndex().use { index ->
            dataset.getTitleQueries().entries.parallelStream().map { (qid, qtext) ->
                val queryJudgments = qrels[qid]
                val qterms = index.tokenize(qtext)

                val SDM = SequentialDependenceModel(qterms, stopwords = inqueryStop)
                println("$qid SDM($qterms)")

                val results = index.search(SDM, depth)

                val rawDocs = results.scoreDocs.associate { sdoc ->
                    val ldoc = index.document(sdoc.doc)!!
                    val fields = pmake {}
                    ldoc.fields.forEach { field ->
                        val name = field.name()!!
                        fields.putIfNotNull(name, field.stringValue())
                        fields.putIfNotNull(name, field.numericValue())
                    }
                    Pair(sdoc.doc, fields)
                }
                val scores = results.scoreDocs.map { it.score.toDouble() }.toDoubleArray()
                val logSumExp = if (scores.isNotEmpty()) MathUtils.logSumExp(scores) else 0.0

                val docPs = results.scoreDocs.mapIndexed { i, sdoc ->
                    val fields = rawDocs[sdoc.doc]!!
                    val docName = fields.getString(index.idFieldName)!!
                    pmake {
                        set("id", docName)
                        set("title-ql", sdoc.score)
                        set("title-ql-prior", Math.exp(sdoc.score - logSumExp))
                        set("rank", i + 1)
                        set("fields", fields)
                    }
                }

                val qjson = pmake {
                    set("qid", qid)
                    set("totalHits", results.totalHits)
                    set("docs", docPs)
                    set("qtext", qtext)
                    set("qterms", qterms)
                }

                val evalResults = QueryResults(results.scoreDocs.mapIndexed { i, sdoc ->
                    val name = rawDocs[sdoc.doc]!!.getString(index.idFieldName)!!
                    SimpleEvalDoc(name, i + 1, sdoc.score.toDouble())
                })

                if (evalResults.isNotEmpty()) {
                    evals.forEach { measure, evalfn ->
                        try {
                            val score = evalfn.evaluate(evalResults, queryJudgments)
                            ms.push("SDM $measure", score)
                            qjson.put(measure, score)
                        } catch (npe: NullPointerException) {
                            System.err.println("NULL in eval...")
                        }
                    }
                }
                qjson
            }.sequential().forEach { qjson ->
                output.println(qjson);
            }
        } // retr
    } // output
    println(ms.means())
}
