package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.CountingDebouncer
import edu.umass.cics.ciir.chai.forEachSeqPair
import edu.umass.cics.ciir.chai.mapEachSeqPair
import edu.umass.cics.ciir.chai.smartPrint
import edu.umass.cics.ciir.iltr.pagerank.SpacesRegex
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.getStr
import edu.umass.cics.ciir.sprf.incr
import edu.umass.cics.ciir.sprf.inqueryStop
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.concurrent.ConcurrentHashMap

/**
 *
 * @author jfoley.
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val field = argp.get("field", "text")
    val normalize = argp.get("normalize", true)

    val output = argp.get("output", "w2v.input")
    argp.set("dataset", "trec-car")

    File(output).smartPrint { writer ->
        if (argp.isString("dataset")) {
            DataPaths.get(argp.getStr("dataset")).getIreneIndex()
        } else {
            IreneIndex(IndexParams().apply {
                withPath(File(argp.getStr("index")))
            })
        }.use { index ->
            val msg = CountingDebouncer(index.totalDocuments.toLong())

            (0 until index.totalDocuments).toList().parallelStream().map { num ->
                val result = index.getField(num, field)?.stringValue() ?: ""
                msg.incr()?.let { upd ->
                    println(upd)
                }
                if (normalize) {
                    index.tokenize(result, field).joinToString(separator = " ")
                } else {
                    result.replace(SpacesRegex, " ")
                }
            }.sequential().forEach { text ->
                if (text.isNotBlank()) {
                    writer.print(text)
                    writer.print("\n\n")
                }
            }
        }
    }
}

data class ContextAndToken(val context: String, val token: String)

inline fun <T> try_or_empty(fn: ()->Collection<T>): Collection<T> {
    try {
        return fn()
    } catch (e: Exception) {
        return emptyList<T>()
    }
}

object CollectStatsForQueries {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val window = argp.get("window", 8)
        val maxItemsInRam = argp.get("inRam", 5_000_000)
        val minFreq = argp.get("minFreq",5)

        val tok = IreneEnglishAnalyzer()
        val tokQueries = DataPaths.QuerySets.flatMap {
            try_or_empty { it.desc_qs.values } + try_or_empty { it.title_qs.values }
        }.map { tok.tokenize("body", it) }

        val stop = inqueryStop
        val unigrams = tokQueries.flatMapTo(HashSet()) { it }.filterNot{ stop.contains(it) }.associate { Pair(it, it) }
        val bigrams = tokQueries.flatMapTo(HashSet()) { qterms ->
            qterms.mapEachSeqPair { lhs, rhs ->
                "$lhs $rhs"
            }
        }.associate { Pair(it, it) }

        println("${unigrams.size} $unigrams")
        println("${bigrams.size} $bigrams")

        val observations = ConcurrentHashMap<ContextAndToken, Int>()
        File("q-context-counts.tsv.gz").smartPrint { writer ->
            DataPaths.TrecCarT200.getIreneIndex().use { index ->
                val field = index.defaultField
                val msg = CountingDebouncer(index.totalDocuments.toLong())
                (0 until index.totalDocuments).toList().parallelStream().forEach { docNo ->
                    val result = index.getField(docNo, field)?.stringValue() ?: ""
                    //val tokens = index.tokenize(result, field)

                    index.analyzer.tokenSequence(field, result).windowed(window,window/2).forEach { slice ->
                        val interesting = slice.filterNot { stop.contains(it) }
                        for (t in slice) {
                            val interned = unigrams[t] ?: continue
                            for (c in interesting) {
                                observations.incr(ContextAndToken(interned, c), 1)
                            }
                        }
                        slice.forEachSeqPair { lhs, rhs ->
                            val interned = bigrams["$lhs $rhs"]
                            if (interned != null) {
                                for (c in interesting) {
                                    observations.incr(ContextAndToken(interned, c), 1)
                                }
                            }
                        }
                    }

                    msg.incr()?.let { upd ->
                        println("${observations.size} $upd")
                    }

                    if (observations.size > maxItemsInRam) {
                        synchronized(observations) {
                            if (observations.size > maxItemsInRam) {
                                val keep = observations.entries.toList()
                                observations.clear()

                                for ((ct, count) in keep) {
                                    if (count <= minFreq) { continue }
                                    writer.println("${ct.context}\t${ct.token}\t${count}")
                                }
                            }
                        }
                    }

                    // Do stuff.
                }
            }
        }
    }

}