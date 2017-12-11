package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.CountingDebouncer
import edu.umass.cics.ciir.chai.forEachSeqPair
import edu.umass.cics.ciir.chai.smartReader
import edu.umass.cics.ciir.chai.splitAt
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

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
}

// Do we find the exact phrasing, and do we
fun GenerateExactMatchQuery(qterms: List<String>, field: String?=null, statsField: String?=null): QExpr {
    if (qterms.isEmpty()) return NeverMatchLeaf

    val phrase = if(qterms.size == 1) {
        TextExpr(qterms[0], field, statsField)
    } else {
        OrderedWindowExpr(qterms.map { TextExpr(it, field, statsField) })
    }
    return AndExpr(listOf(phrase, CountEqualsExpr(LengthsExpr(field), qterms.size)))
}

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
                    println(gTerms)

                    uniqTerms.map { term ->
                        val collection = index.getStats(term)
                        val wikiTitles = wiki.getStats(term, "title")
                        val wikiExact = wiki.getStats(GenerateExactMatchQuery(listOf(term), "title"))
                        val msnCount = msn.getStats(term)
                        val gfe = gTerms[term] ?: 0L

                        val features = Parameters.parseArray(
                                "cf", collection.cf, "df", collection.df,
                                "wt.cf", wikiTitles.cf, "wt.df", wikiTitles.df,
                                "wt.exact", wikiExact.cf > 0,
                                "msn.cf", msnCount.cf, "msn.df", msnCount.df,
                                "gfe", gfe,
                                "inqueryStop", inqueryStop.contains(term))
                        println("$term $features")
                    }
                }
            }
        }


    }
}
