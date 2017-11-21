package edu.umass.cics.ciir.iltr.pagerank

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.irene.example.galagoScrubUrl
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.pmake
import org.lemurproject.galago.utility.Parameters
import java.io.BufferedReader
import java.io.Closeable
import java.io.File
import java.net.URI
import java.net.URISyntaxException

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
val DomainToPageRankFileName = "domainToPageRank.tsv.gz"

object JoinURLToPageRank {
    @JvmStatic fun main(args: Array<String>) {
       val base = File("/mnt/scratch/jfoley/clue12-data")
        val URLMapping = File(base, "ClueWeb12_All_edocid2url.txt.bz2")
        val PageRank = File(base, "pagerank.docNameOrder.bz2")
        val shards = 50

        val total = 733_019_372L;
        var completed = 0L
        val msg = Debouncer()

        ShardWriters(File(base, "url2pr"), shards, DomainToPageRankFileName).use { domainWriters ->
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

object ComputeDomainVectors {
    @JvmStatic fun main(args: Array<String>) {
        val base = File("/mnt/scratch/jfoley/clue12-data/url2pr")
        val msg = Debouncer()
        var completed = 0L

        (0 until 50).forEach { shardId ->
            val shardDir = File(base, "shard$shardId/")
            assert(shardDir.isDirectory && shardDir.exists())
            val domainPageRankStats = HashMap<String, StreamingStats>()
            File(shardDir, DomainToPageRankFileName).smartLines { lines ->
                for (line in lines) {
                    val tab = line.indexOf('\t')
                    if (tab > 0) {
                        val domain = line.substring(0, tab)
                        val pageRank = line.substring(tab+1).toDoubleOrNull() ?: continue
                        domainPageRankStats.computeIfAbsent(domain, {StreamingStats()}).push(pageRank);
                        completed++
                        if(msg.ready()) {
                            println("$shardId ${domainPageRankStats.size} ${msg.estimate(completed, completed)}")
                        }
                    }
                }
            }

            File(shardDir, "domain-vectors.tsv.gz").smartPrint { writer ->
                domainPageRankStats.forEach { domain, stats ->
                    writer.append(domain).append('\t')
                    stats.features.entries.joinTo(writer, separator = "\t") { "${it.key} ${it.value}" }
                    writer.append('\n')

                    if(msg.ready()) {
                        println("$shardId ${domain} ${stats} ${msg.estimate(completed, completed)}")
                    }
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

object FindDomainVectors {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args);
        val dataset = argp.get("dataset", "gov2")
        val ids = File("$dataset.needed.ids").smartReader().lineSequence().toSet()

        val meanDomainStats = StreamingStats()
        val domainStats = File("/mnt/scratch/jfoley/clue12-data/domain-vectors.tsv.gz").smartLines { lines ->
            val loading = CountingDebouncer(4_964_166L)
            lines.associate { line ->
                val cols = line.split('\t', ' ')
                val domain = cols[0]

                val map = (1 until cols.size step 2).associate { i ->
                    val key = cols[i]
                    val value = cols[i+1].toDoubleOrNull() ?: error("Bad parse: ${cols[i+1]}")
                    Pair(key, value)
                }
                val theStats = ComputedStats(map)
                meanDomainStats.push(theStats.mean)
                loading.incr()?.let {
                    println("Loading Domain Vectors: $it")
                }
                Pair(domain, theStats)
            }
        }

        val featurePrefix = "domain-pagerank-"
        val missingStats = meanDomainStats.toComputedStats().toFeatures(featurePrefix)
        missingStats.set("domain-pagerank-is-bg", true)
        File("$dataset.pagerankf.jsonl.gz").smartPrint { writer ->
            val counting = CountingDebouncer(ids.size.toLong())
            DataPaths.get(dataset).getIreneIndex().use { index ->
                ids.parallelStream().map { id ->
                    val internal = index.documentById(id)
                    var foundStats: ComputedStats? = null
                    if (internal != null) {
                        val url = index.getField(internal, "stored-url")?.stringValue()
                        if (url != null) {
                            try {
                                val uri = URI(url)
                                val domain = uri.host
                                if (domain != null) {
                                    foundStats = domainStats[domain]
                                }
                            } catch (err: URISyntaxException) {
                                // Nothing.
                            }
                        }
                    }
                    counting.incr()?.let {
                        println("Processing URLs: $it")
                    }
                    Pair(id, foundStats)
                }.sequential().forEach { (id, stats) ->
                    val fmap = stats?.toFeatures(featurePrefix) ?: missingStats
                    writer.println(pmake {
                        set("id", id)
                        set("features", fmap)
                    })
                }
            }
        }

    }
}