package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.core.parse.TagTokenizer
import org.lemurproject.galago.utility.MathUtils
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator

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
    val depth = 200

    val tok = TagTokenizer()
    StreamCreator.openOutputStream("$dsName.qlpool.jsonl.gz").writer().use { output ->
        dataset.getIndex().use { retrieval ->
            dataset.getTitleQueries().forEach { qid, qtext ->
                val queryJudgments = qrels[qid]
                val qterms = tok.tokenize(qtext).terms
                val lmBaseline = GExpr("combine").apply { addTerms(qterms) }
                println("$qid $lmBaseline")

                val gres = retrieval.transformAndExecuteQuery(lmBaseline, Parameters.create().apply { set("requested", depth) })
                val rawDocs = retrieval.getDocuments(gres.resultSet().toList(), Document.DocumentComponents.All)

                val scores = gres.scoredDocuments.map { it.score }.toDoubleArray()
                val logSumExp = MathUtils.logSumExp(scores)

                val docPs = gres.scoredDocuments.map {sdoc ->
                    val doc = rawDocs[sdoc.name]!!

                    Parameters.create().apply {
                        set("id", sdoc.name)
                        set("title-ql", sdoc.score)
                        set("title-ql-prior", Math.exp(sdoc.score - logSumExp))
                        set("rank", sdoc.rank)
                        set("tokenized", doc.terms.joinToString(separator = " "))
                    }
                }

                val qjson = Parameters.create().apply {
                    set("qid", qid)
                    set("docs", docPs)
                    set("qtext", qtext)
                    set("qterms", qterms)
                }

                evals.forEach { measure, evalfn ->
                    try {
                        val score = evalfn.evaluate(gres.toQueryResults(), queryJudgments)
                        ms.push("LM $measure", score)
                        qjson.put(measure, score)
                    } catch (npe: NullPointerException) {
                        System.err.println("NULL in eval...")
                    }
                }

                output.println(qjson);
            }
        } // retr
    } // output
    println(ms.means())
}
