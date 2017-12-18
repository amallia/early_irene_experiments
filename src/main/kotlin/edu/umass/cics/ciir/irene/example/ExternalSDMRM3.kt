package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.ScoredWord
import edu.umass.cics.ciir.chai.StreamingStats
import edu.umass.cics.ciir.chai.normalize
import edu.umass.cics.ciir.chai.timed
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.toQueryResults
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluator
import edu.umass.cics.ciir.sprf.inqueryStop
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.MathUtils
import org.lemurproject.galago.utility.Parameters
import java.io.File

/**
 * @author jfoley
 */


fun TObjectDoubleHashMap<String>.toList(): List<ScoredWord> {
    val output = ArrayList<ScoredWord>(this.size())
    forEachEntry { term, weight ->
        output.add(ScoredWord(weight.toFloat(), term))
        true
    }
    return output
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "gov2")
    val eName = argp.get("external", "trec-car")
    val dataset = DataPaths.get(dsName)
    val external = DataPaths.get(eName)
    val qrels = dataset.qrels
    val measure = getEvaluator("R1000")
    val info = NamedMeasures()
    val scorer = argp.get("scorer", "ql")
    val qtype = argp.get("qtype", "title")
    val estStats = argp.get("stats", "min")
    val proxType = argp.get("prox", "sc")
    val fbDocs = argp.get("fbDocs", 5)
    val fbTerms = argp.get("fbTerms", 50)
    val fbMix = argp.get("fbMix", 0.7)

    val queries = when(qtype) {
        "title" -> dataset.title_qs
        "desc" -> dataset.desc_qs
        else -> error("qtype=$qtype")
    }
    val scorerFn: (QExpr)->QExpr = when(scorer) {
        "bm25" -> {{ BM25Expr(it, 0.3, 0.9) }}
        "ql" -> {{ DirQLExpr(it) }}
        else -> error("scorer=$scorer")
    }

    val times = StreamingStats()
    val baseTimes = StreamingStats()

    val w1 = File("$dsName.x$eName.sdmrm.$scorer.$qtype.$estStats.trecrun").printWriter()
    val w2 = File("$dsName.x$dsName.sdmrm.$scorer.$qtype.$estStats.trecrun").printWriter()

    val corpus = dataset.getIreneIndex()
    val expansion = external.getIreneIndex()

    corpus.env.estimateStats = if (estStats == "exact") { null } else { estStats }
    expansion.env.estimateStats = if (estStats == "exact") { null } else { estStats }

    queries.forEach { qid, qtext ->
        val judgments = qrels[qid] ?: QueryJudgments(qid, emptyMap())
        if (judgments.wrapped.values.count { it > 0 } == 0) return@forEach
        val qterms = corpus.tokenize(qtext)
        println("$qid $qtext $qterms")

        val baseExpr = SequentialDependenceModel(qterms, stopwords = inqueryStop, makeScorer = scorerFn, outerExpr = ::MeanExpr).map { q ->
            if (q is UnorderedWindowExpr) {
                SmallerCountExpr(q.children)
            } else q
        }

        val (expTime, eExpr) = timed {
            val initial = expansion.search(baseExpr, 1000)
            val fbScDocs = initial.scoreDocs.take(fbDocs)

            val logSumExp = if (fbScDocs.isNotEmpty()) {
                MathUtils.logSumExp(fbScDocs.map { it.score.toDouble() }.toDoubleArray())
            } else {
                0.0
            }
            val rmWeights = TObjectDoubleHashMap<String>()
            fbScDocs.forEach { sdoc ->
                val prior = Math.exp(sdoc.score - logSumExp)
                val terms = expansion.terms(sdoc.doc)
                val length = terms.size.toDouble()
                val freqs = TObjectIntHashMap<String>(terms.size / 2)
                terms.forEach { t ->
                    freqs.adjustOrPutValue(t, 1, 1)
                }
                freqs.forEachEntry { term, count ->
                    val est = (count / length) * prior
                    rmWeights.adjustOrPutValue(term, est, est)
                    true
                }
            }

            val terms = rmWeights.toList()
                    .filterNot { inqueryStop.contains(it.word) }
                    .sortedDescending().take(fbTerms)
            println(terms)

            SumExpr(terms
                    .associate { Pair(it.word, it.score.toDouble()) }
                    .normalize()
                    .map { (k, v) -> DirQLExpr(TextExpr(k)).weighted(v) })
        }
        println("Expansion Time: $expTime")

        val cExpr = baseExpr.mixed(fbMix, eExpr)

        val (timeExact, topExact) = timed { corpus.search(baseExpr, 1000) }
        val exactR = topExact.toQueryResults(corpus, qid)

        val (time, topApprox) = timed { corpus.search(cExpr, 1000) }
        val approxR = topApprox.toQueryResults(corpus, qid)

        times.push(time)
        baseTimes.push(timeExact)

        exactR.outputTrecrun(w1, "sdm")
        approxR.outputTrecrun(w2, "sdm-rm3-external")

        info.push("SDM-R1000", measure.evaluate(exactR, judgments))
        info.push("SDM+ERM3-R1000", measure.evaluate(approxR, judgments))

        println("\t${info} ${times.mean} ${baseTimes.mean}")
    }
    println("\t${info} ${times.mean} ${times}")

    w1.close()
    w2.close()
    corpus.close()
    expansion.close()
}

object EstablishRecallBoost {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dsName = argp.get("dataset", "gov2")
        val dataset = DataPaths.get(dsName)
        val qrels = dataset.qrels
        val measure = getEvaluator("R1000")
        val info = NamedMeasures()
        val scorer = argp.get("scorer", "bm25")
        val qtype = argp.get("qtype", "title")
        val estStats = argp.get("stats", "min")

        val queries = when(qtype) {
            "title" -> dataset.title_qs
            "desc" -> dataset.desc_qs
            else -> error("qtype=$qtype")
        }
        val scorerFn: (QExpr)->QExpr = when(scorer) {
            "bm25" -> {{ BM25Expr(it, 0.3, 0.9) }}
            "ql" -> {{ DirQLExpr(it) }}
            else -> error("scorer=$scorer")
        }

        val times = StreamingStats()
        val baseTimes = StreamingStats()

        val w1 = File("$dsName.nodep.$scorer.$qtype.$estStats.trecrun").printWriter()
        val w2 = File("$dsName.dep.$scorer.$qtype.$estStats.trecrun").printWriter()

        val corpus = dataset.getIreneIndex()
        corpus.env.estimateStats = if (estStats == "exact") { null } else { estStats }

        queries.forEach { qid, qtext ->
            val judgments = qrels[qid] ?: return@forEach
            if (judgments.wrapped.values.count { it > 0 } == 0) return@forEach

            val qterms = corpus.tokenize(qtext)
            println("$qid $qtext $qterms")

            val baseExpr = UnigramRetrievalModel(SmartStop(qterms, stopwords = inqueryStop), scorer = scorerFn)
            val sdmExpr = SequentialDependenceModel(qterms, stopwords = inqueryStop).map { q ->
                if (q is UnorderedWindowExpr) {
                    SmallerCountExpr(q.children)
                } else q
            }

            val (timeExact, topExact) = timed { corpus.search(baseExpr, 1000) }
            val exactR = topExact.toQueryResults(corpus, qid)

            val (time, topApprox) = timed { corpus.search(sdmExpr, 1000) }
            val approxR = topApprox.toQueryResults(corpus, qid)

            times.push(time)
            baseTimes.push(timeExact)

            exactR.outputTrecrun(w1, "no-dep")
            approxR.outputTrecrun(w2, "dep")

            info.push("R-no-dep", measure.evaluate(exactR, judgments))
            info.push("R-dep", measure.evaluate(approxR, judgments))

            println("\t${info} ${times.mean} ${baseTimes.mean}")
        }
        println("\t${info} ${times.mean} ${times}")

        w1.close()
        w2.close()
        corpus.close()
    }
}
