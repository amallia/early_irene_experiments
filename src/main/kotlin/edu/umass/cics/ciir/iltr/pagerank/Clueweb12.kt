package edu.umass.cics.ciir.iltr.pagerank

import edu.umass.cics.ciir.chai.ShardWriters
import edu.umass.cics.ciir.chai.smartReader
import edu.umass.cics.ciir.irene.example.galagoScrubUrl
import java.io.BufferedReader
import java.io.Closeable
import java.io.File
import java.net.URI

/**
 *
 * @author jfoley.
 */
val SpacesRegex = "\\s+".toRegex()

fun <A :Closeable, B: Closeable> Pair<A,B>.use(block: (A,B)->Unit) = {
    this.first.use { a ->
        this.second.use { b ->
            block(a, b)
        }
    }
}

class SortedKVIter(val reader: BufferedReader) : Closeable {
    constructor(path: File) : this(path.smartReader())
    override fun close() = reader.close()
    var done = false
    var nextId: String = ""
    var nextVal: String = ""
    init {
        pull()
    }
    private fun pull() {
        if (done) return
        while (true) {
            val next = reader.readLine()?.split(SpacesRegex)
            if (next == null) {
                done = true
                return
            }
            if (next.size != 2) {
                error("Bad entry: $next")
            }
            nextId = next[0]
            nextVal = next[1]
        }
    }

    fun advanceTo(id: String): Boolean {
        while (nextId < id) {
            pull()
        }
        return nextId == id
    }
}

object JoinURLToPageRank {
    @JvmStatic fun main(args: Array<String>) {
       val base = File("/mnt/scratch/jfoley/clue12-data")
        val URLMapping = File(base, "ClueWeb12_All_edocid2url.txt.bz2")
        val PageRank = File(base, "pagerank.docNameOrder.bz2")
        val shards = 50

        ShardWriters(File(base, "url2pr"), shards, "domainToPageRank.tsv.gz").use { domainWriters ->
            ShardWriters(File(base, "url2pr"), shards, "urlToPageRank.tsv.gz").use { urlWriters ->
                Pair(SortedKVIter(URLMapping), SortedKVIter(PageRank)).use { urls, scores ->
                    while (!urls.done && !scores.done) {
                        if (scores.advanceTo(urls.nextId)) {
                            val score = scores.nextVal
                            val cleanURL = galagoScrubUrl(urls.nextVal) ?: continue
                            urlWriters.hashed(cleanURL).println("$cleanURL\t$score")
                            val domain = URI(cleanURL).host ?: continue
                            domainWriters.hashed(domain).println("$domain\t$score")
                        }
                    }
                }
            }
        }
    }
}