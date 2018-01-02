package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.StreamingStats
import edu.umass.cics.ciir.chai.timed
import edu.umass.cics.ciir.chai.use
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.toQueryResults
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluator
import edu.umass.cics.ciir.sprf.inqueryStop
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File

/**
 *
 * Robust, bm25, min, prox:
 * with idf-extract: AP=0.263, 21.5 ms vs. 41.9 ms
 * without idf-extract: 24.1ms vs. 42.0 ms
 * ql, min, prox:
 * with log-extract: AP=0.257, 27.6 ms vs. 49.4 ms
 * without log-extract: ... about the same
 *
 * With faster-log:
 *	ap1=0.258 ap2=0.257 21.7ms 37.3ms
 *
 * SC is a really neat "optimization" because it *improves* performance on desription queries.
 *
 * @author jfoley.
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "gov2")
    val dataset = DataPaths.get(dsName)
    val qrels = dataset.qrels
    val measure = getEvaluator("map")
    val info = NamedMeasures()
    val scorer = argp.get("scorer", "bm25")
    val qtype = argp.get("qtype", "title")
    val estStats = argp.get("stats", "min")
    val proxType = argp.get("prox", "sc")

    val queries = when(qtype) {
        "title" -> dataset.title_qs
        "desc" -> dataset.desc_qs
        else -> error("qtype=$qtype")
    }
    val scorerFn: (QExpr)->QExpr = when(scorer) {
        "bm25" -> {{BM25Expr(it, 0.3, 0.9)}}
        "ql" -> {{DirQLExpr(it)}}
        else -> error("scorer=$scorer")
    }

    val times = StreamingStats()
    val baseTimes = StreamingStats()

    Pair(File("$dsName.sdm.$scorer.$qtype.$estStats.trecrun").printWriter(), File("$dsName.sdm-${proxType}.$scorer.$qtype.$estStats.trecrun").printWriter()).use { w1, w2 ->
        dataset.getIreneIndex().use { index ->
            index.env.estimateStats = if (estStats == "exact") { null } else { estStats }
            queries.forEach { qid, qtext ->
                val qterms = index.tokenize(qtext)
                val judgments = qrels[qid] ?: QueryJudgments(qid, emptyMap())

                // Prox does nothing for queries with single term.
                if (judgments.size > 0 && qterms.size > 1) {
                    println("$qid $qtext $qterms")

                    val sdmQ = SequentialDependenceModel(qterms, makeScorer = scorerFn)
                    val approxSDMQ = SequentialDependenceModel(qterms, stopwords = inqueryStop, makeScorer = scorerFn).map { q ->
                        // Swap UW nodes with Prox nodes.
                        if (q is UnorderedWindowExpr) {
                            when (proxType) {
                                "sc" -> SmallerCountExpr(q.children)
                                "prox" -> ProxExpr(q.children, q.width)
                                else -> error("No type $proxType!")
                            }
                        } else {
                            q
                        }
                    }

                    // cache term statistics so timing is fair.
                    sdmQ.visit { it.applyEnvironment(index.env) }
                    approxSDMQ.visit { it.applyEnvironment(index.env) }

                    //val (timeExact, topExact) = timed { index.search(sdmQ, 1000) }
                    //val exactR = topExact.toQueryResults(index, qid)
                    val (time, topApprox) = timed {index.search(approxSDMQ, 1000)}
                    val approxR = topApprox.toQueryResults(index, qid)

                    times.push(time)
                    //baseTimes.push(timeExact)

                    //exactR.outputTrecrun(w1, "sdm")
                    approxR.outputTrecrun(w2, "sdm-$proxType")

                    //info.push("ap1", measure.evaluate(exactR, judgments))
                    info.push("ap2", measure.evaluate(approxR, judgments))

                    println("\t${info} ${times.mean} ${baseTimes.mean}")
                }
            }
        }
        println("\t${info} ${times.mean} ${times}")
    }

}