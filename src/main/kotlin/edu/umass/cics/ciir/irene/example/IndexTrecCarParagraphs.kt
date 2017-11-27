package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.CountingDebouncer
import edu.umass.cics.ciir.chai.Debouncer
import edu.umass.cics.ciir.chai.smartDoLines
import edu.umass.cics.ciir.iltr.pagerank.SpacesRegex
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluators
import edu.unh.cs.treccar.Data
import edu.unh.cs.treccar.read_data.DeserializeData
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File
import java.net.URLDecoder

fun getTrecCarIndexParams(path: File) = IndexParams().apply {
    withPath(path)
    defaultField = "text"
    withAnalyzer("links", WhitespaceAnalyzer())
}

/**
 *
 * @author jfoley.
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val paragraphsInput = File(argp.get("input", "data/test200/train.test200.fold0.cbor.paragraphs"))
    val indexPath = File(argp.get("output", "data/test200.fold0.irene2"))

    val msg = Debouncer()
    // Seven million paragraphs (as described in paper)
    // much more in actual 16GB dump
    val total = argp.get("total", 29678367L)

    IreneIndexer(getTrecCarIndexParams(indexPath).apply { create() }).use { writer ->
        StreamCreator.openInputStream(paragraphsInput).use { input ->
            DeserializeData.iterParagraphs(input).forEach { paragraph: Data.Paragraph ->
                val id = paragraph.paraId
                val text = paragraph.textOnly
                val links = paragraph.entitiesOnly

                val processed = writer.push(
                        StringField("id", id, Field.Store.YES),
                        TextField("text", text, Field.Store.YES),
                        TextField("links", links.joinToString(separator="\t"), Field.Store.YES)
                )

                if (msg.ready()) {
                    println(id)
                    println(links)
                    println(text)
                    println(msg.estimate(processed, total))
                }
            }
        }
    }
}

data class TrecCarJudgment(val qid: String, val paragraphId: String, val judgment: Int=1) {
    val queryParts: List<String>
            get() = qid.split('/').map { URLDecoder.decode(it, "UTF-8") }.reversed()
    val page: String
        get() = queryParts.last()
}

data class TrecCarDataset(val queries: Map<String, String>, val judgments: QuerySetJudgments)
fun loadTrecCarDataset(qrelsPath: File): TrecCarDataset {
    val judgments = ArrayList<TrecCarJudgment>()
    qrelsPath.smartDoLines { line ->
        val cols = line.split(SpacesRegex)
        if (cols.size == 4) {
            val qid = cols[0]
            //val unused = cols[1]
            val paragraphId = cols[2]
            val judgment = cols[3].toInt()
            judgments.add(TrecCarJudgment(qid, paragraphId, judgment))
        }
    }

    val queries = judgments.associate { Pair(it.qid, it.queryParts.joinToString(separator = "\t")) }
    val qrels = QuerySetJudgments(
            judgments.groupBy { it.qid }
                    .mapValues { (qid, v) ->
                        QueryJudgments(qid, v.associate { Pair(it.paragraphId, it.judgment) })
                    }
    )
    return TrecCarDataset(queries, qrels)
}

object CountDocuments {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        IreneIndex(getTrecCarIndexParams(File(argp.get("index", "/mnt/scratch/jfoley/trec-car/paragraphs.irene2")))).use { index ->
            println(index.totalDocuments)
        }
    }
}

object Test200Baseline {
    @JvmStatic fun main(args: Array<String>) {
        val basePath = File("/mnt/scratch/jfoley/trec-car/")
        val argp = Parameters.parseArgs(args)
        val qrelsPath = File(argp.get("qrels", File(basePath, "test200/train.test200.fold0.cbor.hierarchical.qrels").absolutePath))
        val measures = getEvaluators("map", "recip_rank", "r-prec")
        val summary = NamedMeasures()

        val (queries, qrels) = loadTrecCarDataset(qrelsPath)
        println(queries.size)

        IreneIndex(getTrecCarIndexParams(File(argp.get("index", "/mnt/scratch/jfoley/trec-car/paragraphs.irene2")))).use { index ->
            index.env.estimateStats = "min"
            val msg = CountingDebouncer(queries.size.toLong())
            queries.forEach { (qid, qtext) ->
                val qj = qrels[qid] ?: error("No judgments for $qid")
                val terms = index.tokenize(qtext)
                //val qsdm = SequentialDependenceModel(terms, stopwords = inqueryStop)
                val ql = QueryLikelihood(terms)

                val results = index.search(ql, 100)
                val gres = results.toQueryResults(index)

                val computed = measures.mapValues { (_, fn) -> fn.evaluate(gres, qj) }

                synchronized(summary) {
                    computed.forEach { m, score -> summary.push(m, score) }

                    msg.incr()?.let { upd ->
                        println(summary)
                        println(upd)
                    }
                }

            }
        }

    }
}