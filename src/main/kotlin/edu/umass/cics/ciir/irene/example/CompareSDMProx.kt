package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.use
import edu.umass.cics.ciir.irene.lang.DirQLExpr
import edu.umass.cics.ciir.irene.lang.SequentialDependenceModel
import edu.umass.cics.ciir.irene.lang.SmallerCountExpr
import edu.umass.cics.ciir.irene.lang.UnorderedWindowExpr
import edu.umass.cics.ciir.irene.toQueryResults
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluator
import org.lemurproject.galago.utility.Parameters
import java.io.File

/**
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
    val estStats = argp.get("stats", "min")

    Pair(File("$dsName.sdm.$estStats.trecrun").printWriter(), File("$dsName.sdm-sc.$estStats.trecrun").printWriter()).use { w1, w2 ->
        dataset.getIreneIndex().use { index ->
            index.env.estimateStats = estStats
            dataset.title_qs.forEach { qid, qtext ->
                val qterms = index.tokenize(qtext)

                // Prox does nothing for queries with single term.
                if (qterms.size > 1) {
                    println(qid)

                    val sdmQ = SequentialDependenceModel(qterms)
                    val approxSDMQ = sdmQ.deepCopy()

                    approxSDMQ.visit { q ->
                        // Swap UW nodes with Prox nodes.
                        if (q is DirQLExpr && q.child is UnorderedWindowExpr) {
                            val uw = q.child as? UnorderedWindowExpr ?: error("Concurrent Access.")
                            //q.child = ProxExpr(uw.deepCopyChildren(), uw.width)
                            q.child = SmallerCountExpr(uw.deepCopyChildren())
                        }
                    }

                    val exactR = index.search(sdmQ, 1000).toQueryResults(index, qid)
                    val approxR = index.search(approxSDMQ, 1000).toQueryResults(index, qid)

                    exactR.outputTrecrun(w1, "sdm")
                    approxR.outputTrecrun(w2, "sdm-sc")

                    info.push("ap1", measure.evaluate(exactR, qrels[qid]))
                    info.push("ap2", measure.evaluate(approxR, qrels[qid]))

                    println("\t${info}")
                }
            }
        }
    }
}