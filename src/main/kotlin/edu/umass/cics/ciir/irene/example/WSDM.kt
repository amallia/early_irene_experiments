package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.CountingDebouncer
import edu.umass.cics.ciir.chai.smartReader
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
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

    @JvmStatic fun main(args: Array<String>) {
        val p = IndexParams().apply {
            withPath(File(dir, "msn-qlog.irene2"))
            defaultField = "query"
            create()
        }

        IreneIndexer(p).use { indexer ->
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
                        setTextField(p.defaultField, queryText)
                    }

                    msg.incr()?.let { upd ->
                        println(upd)
                    }
                }
            }
        }
    }

}

