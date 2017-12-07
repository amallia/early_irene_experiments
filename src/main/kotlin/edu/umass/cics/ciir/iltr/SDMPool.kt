package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.chai.safeDiv
import edu.umass.cics.ciir.chai.smartPrint
import edu.umass.cics.ciir.irene.lang.BM25Model
import edu.umass.cics.ciir.irene.lang.QueryLikelihood
import edu.umass.cics.ciir.irene.lang.SequentialDependenceModel
import edu.umass.cics.ciir.irene.lang.SmartStop
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.inqueryStop
import edu.umass.cics.ciir.sprf.pmake
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "nyt-cite")
    val dataset = DataPaths.get(dsName)
    val qrels = dataset.getQueryJudgments()
    val ms = NamedMeasures()
    val depth = argp.get("depth", 500)

    File("$dsName.irene.pool.jsonl.gz").smartPrint { output ->
        dataset.getIreneIndex().use { index ->
            index.env.estimateStats = "min"
            dataset.getTitleQueries().entries.parallelStream().map { (qid, qtext) ->
                val queryJudgments = qrels[qid] ?: QueryJudgments(qid, emptyMap())
                val relDocs = queryJudgments.filterValues { it > 0 }.keys.filterNotNullTo(HashSet())
                val qterms = index.tokenize(qtext)

                val poolingQueries = hashMapOf(
                        "sdm-stop" to SequentialDependenceModel(qterms, stopwords = inqueryStop),
                        "ql" to QueryLikelihood(qterms),
                        "bm25" to BM25Model(qterms)
                )
                val sterms = SmartStop(qterms, inqueryStop)
                if (sterms.size != qterms.size) {
                    poolingQueries["ql-stop"] = QueryLikelihood(sterms)
                    poolingQueries["bm25-stop"] = BM25Model(sterms)
                }

                val results = index.pool(poolingQueries, depth)
                val inPool = results.values.flatMapTo(HashSet()) { it.scoreDocs.map { it.doc } }

                val relFound = inPool.count { relDocs.contains(index.getDocumentName(it)) }
                println("qid:$qid, R:${safeDiv(relFound, relDocs.size)} R:$relFound/${relDocs.size}")

                // Not helpful for learning otherwise...
                if (relFound > 0) {
                    val rawDocs = inPool.associate { doc ->
                        val ldoc = index.document(doc)!!
                        val fields = pmake {}
                        ldoc.fields.forEach { field ->
                            val name = field.name()!!
                            fields.putIfNotNull(name, field.stringValue())
                            fields.putIfNotNull(name, field.numericValue())
                        }
                        Pair(doc, fields)
                    }
                    val docPs = inPool.map { id ->
                        val fields = rawDocs[id]!!
                        val docName = fields.getString(index.idFieldName)!!
                        pmake {
                            set("id", docName)
                            set("fields", fields)
                        }
                    }

                    val qjson = pmake {
                        set("qid", qid)
                        set("docs", docPs)
                        set("qtext", qtext)
                        set("qterms", qterms)
                    }


                    qjson
                } else null
            }.sequential().forEach { qjson ->
                if (qjson != null) {
                    output.println(qjson);
                }
            }
        } // retr
    } // output
    println(ms.means())
}
