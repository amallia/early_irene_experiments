package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.lang.TextExpr
import edu.umass.cics.ciir.irene.lang.UnorderedWindowExpr
import edu.umass.cics.ciir.irene.lang.generateExactMatchQuery
import edu.umass.cics.ciir.irene.lang.phraseQuery
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.collections.HashSet

/**
 * @author jfoley
 */
object PrepareMSNQLogDB {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val columns = listOf("Time", "Query", "QueryID", "SessionID", "ResultCount")
    val dir = File("/mnt/scratch/jfoley/msn-qlog/")
    val NLines = 14921286
    val NQ = NLines-1
    val log = File(dir, "srfp20060501-20060531.queries.txt.gz")

    val params = IndexParams().apply {
        withPath(File(dir, "msn-qlog.irene2"))
        defaultField = "query"
        create()
    }

    @JvmStatic fun main(args: Array<String>) {
        IreneIndexer(params).use { indexer ->
            log.smartReader().useLines { lines ->
                val iterLines = lines.iterator()
                val header = iterLines.next().split("\t")
                val qIndex = header.indexOf("Query")
                if (qIndex < 0) error("Couldn't find `Query' heading in $header")

                val msg = CountingDebouncer(NQ.toLong())
                while (iterLines.hasNext()) {
                    val cols = iterLines.next().split("\t")
                    val time = cols[0]
                    val queryText = cols[1]
                    val qid = cols[2]
                    val session = cols[3]
                    val resultCount = cols[4].toInt()

                    val ldt = LocalDateTime.parse(time, formatter)
                    val millis = ldt.atOffset(ZoneOffset.UTC).toEpochSecond()

                    indexer.doc {
                        setId(qid)
                        setStringField("session", session)
                        setDenseIntField("resultCount", resultCount)
                        setDenseLongField("time", millis)
                        setTextField(params.defaultField, queryText)
                    }

                    msg.incr()?.let { upd ->
                        println(upd)
                    }
                }
            }
        }
    }
}

object GoogleNGrams {
    val dir = when(IRDataset.host) {
        "oakey" -> File("/mnt/scratch/jfoley/google-n-grams-T13")
        "sydney" -> File("/mnt/nfs/collections/google-n-grams-T13")
        else -> notImpl(IRDataset.host)
    }
    val numTerms = 1_024_908_267_229L
    val numUnigramLines = 13_588_391

    fun pullUnigramStats(request: Set<String>): Map<String, Long> {
        val terms = request.toHashSet()
        val outputFreqs = HashMap<String, Long>()

        val reader = File(dir, "1gms/vocab.gz").smartReader()
        val msg = CountingDebouncer(total=numUnigramLines.toLong())
        while(terms.isNotEmpty()) {
            val (term, countStr) = reader.readLine()?.splitAt('\t') ?: break
            if (terms.remove(term)) {
                outputFreqs.put(term, countStr.toLong())
            }
            msg.incr()?.let { upd ->
                println("pullUnigramStats ${outputFreqs.size}/${request.size} ... $upd")
            }
        }

        return outputFreqs
    }

    val numBigramLines = 10_000_000L * 31

    fun pullBigramStats(request: Set<Pair<String, String>>): Map<Pair<String,String>, Long> {
        val terms = request.toHashSet()
        val outputFreqs = HashMap<Pair<String, String>, Long>()

        val inputs =  File(dir, "2gms").listFiles().filter { it.name.startsWith("2gm-00") && it.name.endsWith(".gz") }.toCollection(LinkedList())
        println(inputs)
        //val inputs = inputFiles.map { it.smartReader() }.toCollection(LinkedList())

        val msg = CountingDebouncer(total= numBigramLines)
        while(terms.isNotEmpty() && inputs.isNotEmpty()) {
            val inputF = inputs.pop() ?: break
            inputF.smartReader().use { reader ->
                while (terms.isNotEmpty()) {
                    val (phrase, countStr) = reader.readLine()?.splitAt('\t') ?: break
                    val bigram = phrase.splitAt(' ') ?: continue
                    if (terms.remove(bigram)) {
                        outputFreqs.put(bigram, countStr.toLong())
                    }
                    msg.incr()?.let { upd ->
                        println("pullBigramStats ${outputFreqs.size}/${request.size} ... $upd")
                    }
                }
            }
        }


        return outputFreqs
    }
}

fun proxQuery(terms: List<String>, field: String? = null, statsField: String? = null) = UnorderedWindowExpr(terms.map { TextExpr(it, field, statsField) })

object CollectWSDMFeatures {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dsName = argp.get("dataset", "gov2")
        val dataset = DataPaths.get(dsName)
        val output = File("$dsName.wsdmf.json")

        WikiSource().getIreneIndex().use { wiki ->
            IreneIndex(PrepareMSNQLogDB.params).use { msn ->
                dataset.getIreneIndex().use { index ->
                    val uniqTerms = HashSet<String>()
                    val uniqPairs = HashSet<Pair<String,String>>()
                    (dataset.title_qs.values + dataset.desc_qs.values).forEach { query ->
                        val qterms = index.tokenize(query)
                        uniqTerms.addAll(qterms)
                        qterms.forEachSeqPair { lhs, rhs ->
                            uniqPairs.add(Pair(lhs, rhs))
                        }
                    }

                    val gTerms = GoogleNGrams.pullUnigramStats(uniqTerms)
                    val swappedOrder = uniqPairs.flatMapTo(HashSet()) { listOf(it, Pair(it.second, it.first)) }
                    val bgTerms = GoogleNGrams.pullBigramStats(swappedOrder)
                    println(gTerms)

                    val uniF = uniqTerms.pmap { term ->
                        val collection = index.getStats(term)
                        val wikiTitles = wiki.getStats(term, "title")
                        val wikiExact = wiki.getStats(generateExactMatchQuery(listOf(term), "title"))
                        val msnCount = msn.getStats(term)
                        val msnExact = msn.getStats(generateExactMatchQuery(listOf(term)))
                        val gfe = gTerms[term] ?: 0L

                        val features = Parameters.parseArray(
                                "cf", collection.cf, "df", collection.df,
                                "wt.cf", wikiTitles.cf, "wt.df", wikiTitles.df,
                                "wt.exact", wikiExact.cf,
                                "msn.cf", msnCount.cf, "msn.df", msnCount.df,
                                "msn.exact", msnExact.cf,
                                "gfe", gfe,
                                "inqueryStop", inqueryStop.contains(term))
                        println("$term $features")

                        Pair(term, features)
                    }.associate { it }

                    val msg = CountingDebouncer(uniqPairs.size.toLong()*2)
                    val uwF = uniqPairs.pmap { (lhs, rhs) ->
                        val collection = index.getStats(proxQuery(listOf(lhs, rhs)))
                        val wikiTitles = wiki.getStats(proxQuery(listOf(lhs, rhs), "title"))
                        val wikiExact = wiki.getStats(generateExactMatchQuery(listOf(lhs, rhs), "title"))
                        val msnCount = msn.getStats(proxQuery(listOf(lhs, rhs)))
                        val msnExact = msn.getStats(generateExactMatchQuery(listOf(lhs, rhs)))
                        var gfe = (bgTerms[Pair(lhs, rhs)] ?: 0L) + (bgTerms[Pair(rhs, lhs)] ?: 0L)
                        if (gfe == 0L)
                            gfe = minOf(gTerms[lhs] ?: gTerms[rhs] ?: 0L, gTerms[rhs] ?: gTerms[lhs] ?: 0L)

                        val features = Parameters.parseArray(
                                "cf", collection.cf, "df", collection.df,
                                "wt.cf", wikiTitles.cf, "wt.df", wikiTitles.df,
                                "wt.exact", wikiExact.cf,
                                "msn.cf", msnCount.cf, "msn.df", msnCount.df,
                                "msn.exact", msnExact.cf,
                                "gfe", gfe,
                                "inqueryStop", inqueryStop.contains(lhs) && inqueryStop.contains(rhs))

                        msg.incr()?.let { upd ->
                            println("uw: $lhs $rhs $features")
                            println(upd)
                        }

                        Pair("$lhs $rhs", features)
                    }.associate { it }

                    val biF = uniqPairs.pmap { (lhs, rhs) ->
                        val collection = index.getStats(phraseQuery(listOf(lhs, rhs)))
                        val wikiTitles = wiki.getStats(phraseQuery(listOf(lhs, rhs), "title"))
                        val wikiExact = wiki.getStats(generateExactMatchQuery(listOf(lhs, rhs), "title"))
                        val msnCount = msn.getStats(phraseQuery(listOf(lhs, rhs)))
                        val msnExact = msn.getStats(generateExactMatchQuery(listOf(lhs, rhs)))
                        val gfe = bgTerms[Pair(lhs, rhs)] ?: 0L

                        val features = Parameters.parseArray(
                                "cf", collection.cf, "df", collection.df,
                                "wt.cf", wikiTitles.cf, "wt.df", wikiTitles.df,
                                "wt.exact", wikiExact.cf,
                                "msn.cf", msnCount.cf, "msn.df", msnCount.df,
                                "msn.exact", msnExact.cf,
                                "gfe", gfe,
                                "inqueryStop", inqueryStop.contains(lhs) && inqueryStop.contains(rhs))
                        msg.incr()?.let { upd ->
                            println("bi: $lhs $rhs $features")
                            println(upd)
                        }

                        Pair("$lhs $rhs", features)
                    }.associate { it }

                    val features = Parameters.create()
                    features.put("t", Parameters.wrap(uniF))
                    features.put("od", Parameters.wrap(biF))
                    features.put("uw", Parameters.wrap(uwF))

                    output.smartPrint { writer ->
                        writer.println(features.toPrettyString())
                    }
                }
            }
        }


    }
}
