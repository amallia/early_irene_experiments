package edu.umass.cics.ciir.iltr.pagerank

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.irene.example.galagoScrubUrl
import org.lemurproject.galago.utility.Parameters
import java.io.BufferedReader
import java.io.Closeable
import java.io.File
import java.net.URI

/**
 *
 * @author jfoley.
 */
val SpacesRegex = "\\s+".toRegex()


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

    fun next() { pull() }
    fun advanceTo(id: String): Boolean {
        while (!done && nextId < id) {
            pull()
        }
        return nextId == id
    }
}
val URLToPageRankFileName = "urlToPageRank.tsv.gz"

object JoinURLToPageRank {
    @JvmStatic fun main(args: Array<String>) {
       val base = File("/mnt/scratch/jfoley/clue12-data")
        val URLMapping = File(base, "ClueWeb12_All_edocid2url.txt.bz2")
        val PageRank = File(base, "pagerank.docNameOrder.bz2")
        val shards = 50

        val total = 733_019_372L;
        var completed = 0L
        val msg = Debouncer()

        ShardWriters(File(base, "url2pr"), shards, "domainToPageRank.tsv.gz").use { domainWriters ->
            ShardWriters(File(base, "url2pr"), shards, URLToPageRankFileName).use { urlWriters ->
                Pair(SortedKVIter(URLMapping), SortedKVIter(PageRank)).use { urls, scores ->
                    assert(!urls.done)
                    assert(!scores.done)
                    print("Starting: urls@${urls.nextId} scores@${scores.nextId}")

                    while (!urls.done && !scores.done) {
                        if (scores.advanceTo(urls.nextId)) {
                            val score = scores.nextVal
                            val cleanURL = galagoScrubUrl(urls.nextVal)
                            if (cleanURL == null) {
                                urls.next()
                                continue
                            }
                            completed++
                            if (msg.ready()) {
                                println(msg.estimate(completed, total))
                            }
                            urlWriters.hashed(cleanURL).println("$cleanURL\t$score")
                            try {
                                val domain = URI(cleanURL).host
                                if (domain != null) {
                                    domainWriters.hashed(domain).println("$domain\t$score")
                                }
                            } catch (e: Exception) {
                                // Can't parse the URL
                            }
                        }
                        urls.next()
                    }

                    print("Ending: urls@${urls.nextId} scores@${scores.nextId}")
                    println(msg.estimate(completed, total))
                }
            }
        }
    }
}

object FindNeededPageRanks {
    // Note to self, this approach is very poor since they mostly don't exist anymore! At least in exactitude. Better just create domain vectors.
    @JvmStatic fun main(args: Array<String>) {
        val base = File("/mnt/scratch/jfoley/clue12-data/url2pr")
        val shards = 50
        val argp = Parameters.parseArgs(args);
        val dataset = argp.get("dataset", "gov2")
        val ids = File("$dataset.needed.ids").smartReader().lineSequence().toSet()

        val urlToId = HashMap<String, String>()
        File("/mnt/scratch/jfoley/$dataset.links/urls.tsv.gz").smartDoLines(true) { line ->
            val cols = line.split('\t')
            if (ids.contains(cols[0])) {
                urlToId[cols[2]] = cols[0]
            }
        }

        println("Found urls for ${urlToId.size} item of ${ids.size} needed.")

        val urlsByShard = HashMap<Int, MutableList<String>>()
        urlToId.forEach { url, _ ->
            urlsByShard
                    .computeIfAbsent(ShardWritersHash(url, shards)) { ArrayList() }
                    .add(url)
        }

        val msg = Debouncer()
        var completed = 0L
        val total = urlToId.size.toLong()
        File("$dataset.pagerank.tsv.gz").smartPrint { writer ->
            urlsByShard.forEach { shardId, urls ->
                println("Begin shard $shardId with ${urls.size} to find...")
                val urlSet = urls.toHashSet()
                File(base, "shard$shardId/$URLToPageRankFileName").smartLines {  lines ->
                    for (line in lines) {
                        val tab = line.indexOf('\t')
                        val url = line.substring(0, tab)
                        if (urlSet.remove(url)) {
                            val id = urlToId[url]!!
                            writer.println("$id\t$line")
                            completed++
                        }
                        if (urlSet.isEmpty()) {
                            println("Finished early!")
                            break
                        }
                        if (msg.ready()) {
                            println(msg.estimate(completed, total))
                        }
                    }
                }
                // finish using lines
            }
        }
    }
}