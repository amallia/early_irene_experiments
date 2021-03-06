package edu.umass.cics.ciir.sprf.example

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneEnglishAnalyzer
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.galago.*
import edu.umass.cics.ciir.irene.tokenize
import edu.umass.cics.ciir.irene.utils.*
import edu.umass.cics.ciir.sprf.*
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexableField
import org.jsoup.Jsoup
import org.jsoup.safety.Cleaner
import org.jsoup.safety.Whitelist
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.logging.FileHandler
import java.util.logging.Level
import java.util.logging.Logger
import java.util.logging.SimpleFormatter

typealias JsoupDoc = org.jsoup.nodes.Document

/**
 * @author jfoley
 */

fun FileLogger(path: String) = Logger.getAnonymousLogger().apply {
    useParentHandlers = false
    addHandler(FileHandler(path).apply {
        formatter = SimpleFormatter()
    })
}

val PunctuationRegex = "\\p{Punct}".toRegex()

fun processDoc(gdoc: GDoc, writer: IreneIndexer, logger: Logger) {
    val id = gdoc.name!!
    val meta = gdoc.metadata ?: emptyMap()
    val url = meta["url"]
    val text = gdoc.text
    val html = try {
        Jsoup.parse(text)
    } catch (e: Throwable) {
        logger.log(Level.WARNING, "Jsoup Exception", e)
        return
    }

    val doc = arrayListOf<IndexableField>()
    doc.add(StringField("id", id, Field.Store.YES))
    if (!url.isNullOrBlank()) {
        doc.add(StringField("stored-url", url, Field.Store.YES))
        val tokenized = url!!.split(PunctuationRegex).joinToString(" ")
        doc.add(TextField("url", tokenized, Field.Store.YES))
    }

    val title = html.title()
    if (!title.isNullOrBlank()) {
        doc.add(TextField("title", title, Field.Store.YES))
    }
    val body = html.body()?.text()
    if (!body.isNullOrBlank()) {
        doc.add(TextField("body", body, Field.Store.YES))
    }
    doc.add(TextField("document", html.text(), Field.Store.YES))

    writer.push(doc)
    //println("$id $title $url")
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "gov2")
    val dataset = DataPaths.get(dsName)

    val indexF = File(argp.get("path", "$dsName.irene2"))
    val params = IndexParams().apply {
        create()
        withPath(indexF)
        withAnalyzer("url", WhitespaceAnalyzer())
    }
    val logger = FileLogger(argp.get("logger", "${indexF.absolutePath}.log"))!!

    val pool = ForkJoinPool.commonPool()!!

    IreneIndexer(params).use { writer ->
        dataset.getIndex().use { retr ->
            retr.forAllGDocs { gdoc ->
                // Thread parsing!
                pool.submit { processDoc(gdoc, writer, logger) }
            } // all docs

            // wait for job to finish.
            while(!pool.awaitQuiescence(1, TimeUnit.SECONDS)) { }
        } // close index
    } // close writer
}


fun galagoScrubUrl(input: String?): String? {
    var url = input ?: return null
    if (url.isEmpty()) return null
    // remove a leading pound sign
    if (url[url.length - 1] == '#') {
        url = url.substring(0, url.length - 1)        // make it lowercase
    }
    url = url.toLowerCase()

    // remove a port number, if it's the default number
    url = url.replace(":80/", "/")
    if (url.endsWith(":80")) {
        url = url.replace(":80", "")
    }
    // remove trailing slashes
    while (url[url.length - 1] == '/') {
        url = url.substring(0, url.length - 1)
    }
    return url.toLowerCase()
}

sealed class ExtractLinksRecord
data class DocHasURLRecord(val id: String, val url: String, val outlinks: Int): ExtractLinksRecord() {
    constructor(p: Parameters): this(p.getString("id")!!, p.getString("url")!!, p.getInt("outlinks"))
}
data class AnchorRecord(val id: String, val dest: String, val text: String): ExtractLinksRecord() {
    constructor(p: Parameters): this(p.getString("id")!!, p.getString("dest")!!, p.getString("text")!!)
}
private fun parseRecord(p: Parameters): ExtractLinksRecord {
    if (p.isString("dest")) return AnchorRecord(p)
    return DocHasURLRecord(p)
}

object ExtractLinks {
    fun String?.limit(n: Int): String? {
        if (this == null) return null
        if (this.length > n) {
            return this.substring(n)
        }
        return this
    }
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dsName = argp.get("dataset", "gov2")
        val dataset = DataPaths.get(dsName)
        val outDir = File(argp.get("output", "$dsName.links"))
        val anchorLimit = argp.get("maxAnchorSize", 1 shl 12);
        val shards = argp.get("shards", 50)

        val logger = FileLogger(argp.get("logger", "${outDir.absolutePath}.log"))

        StreamCreator.openOutputStream(File(outDir, "urls.tsv.gz")).printer().use { urlTable ->
            ShardWriters(outDir, shards, "inlinks.jsonl.gz").use { writers ->
                dataset.getIndex().use { retr ->
                    retr.forAllGDocs { gdoc ->
                        val id = gdoc.name!!
                        val meta = gdoc.metadata ?: emptyMap()
                        val url = galagoScrubUrl(meta["url"])


                        val html = try {
                            Jsoup.parse(gdoc.text)
                        } catch (e: Throwable) {
                            logger.log(Level.WARNING, "Jsoup Exception", e)
                            return@forAllGDocs
                        }

                        val outlinks = html.select("a")
                        urlTable.println("$id\t${outlinks.size}\t${url}")
                        // send this url=id entry to the appropriate shard
                        writers.hashed(url).println(pmake {
                            set("id", id)
                            set("url", url)
                            set("outlinks", outlinks.size)
                        })

                        outlinks.forEach { anchor ->
                            val dest = galagoScrubUrl(anchor.attr("abs:href")) ?: ""
                            val text = anchor.text().limit(anchorLimit) ?: ""

                            // send this "from id with text" to appropriate shard.
                            if (!dest.isBlank()) {
                                writers.hashed(dest).println(pmake {
                                    set("id", id)
                                    set("dest", dest)
                                    set("text", text)
                                })
                            }

                        }
                    }
                }
            }
        }
    }
}

data class DocRepr(val id: String, val text: MutableList<String> = ArrayList<String>(), val neighbors: MutableList<String> = ArrayList<String>()) {
    fun toJSON() = pmake {
        set("id", id)
        set("anchor_texts", text)
        set("inlink_neighbors", neighbors.joinToString(separator = " "))
    }
}

object ExtractLinksReduce {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dsName = argp.get("dataset", "wt10g")
        val outDir = File(argp.get("output", "$dsName.links"))
        val shards = argp.get("shards", 50)
        val shardId = argp.get("shardId", 0)

        val shardDir = File(outDir, "shard$shardId")

        val urlToDoc = HashMap<String, DocRepr>()
        val inputFile = File(shardDir, "/inlinks.jsonl.gz")
        inputFile.smartDoLines { line ->
            val record = parseRecord(Parameters.parseString(line))
            when(record) {
                is DocHasURLRecord -> urlToDoc.put(record.url, DocRepr(record.id))
                else -> {}
            }
        }
        File(shardDir, "edges.tsv.gz").smartPrint { edges ->
            inputFile.smartDoLines { line ->
                val record = parseRecord(Parameters.parseString(line))
                when(record) {
                    is AnchorRecord -> {
                        val found = urlToDoc[record.dest]
                        if (found != null) {
                            // in-collection edges only.
                            edges.println("${record.id}\t${found.id}")
                            // Save edge? [record.id
                            if (record.text.isNotBlank()) {
                                found.text.add(record.text)
                            }
                            found.neighbors.add(record.id)
                        }
                    }
                    else -> {}
                }
            }
        }
        File(shardDir, "docs.jsonl.gz").smartPrint { docs ->
            urlToDoc.forEach { url, repr ->
                if (repr.neighbors.isNotEmpty() || repr.text.isNotEmpty()) {
                    docs.println(repr.toJSON())
                }
            }
        }
    }
}

val MyWhitelist = Cleaner(Whitelist.relaxed().apply {
    removeAttributes("a", "href")
})
fun computeHTMLStaticFeatures(logger: Logger, raw_text: String?, url: String, parsed_html: JsoupDoc? = null): Parameters {
    val analyzer = IreneEnglishAnalyzer()
    val html = parsed_html ?: try {
        Jsoup.parse(raw_text ?: error("Must provide parsed_html or raw_text."))!!
    } catch (e: Throwable) {
        logger.log(Level.WARNING, "Jsoup Exception", e)
        return pmake {
            set("jsoup_error", true)
        }
    }

    val allTerms = analyzer.tokenize("body", html.text())

    val visTerms = try {
        analyzer.tokenize("body", MyWhitelist.clean(html).body().text())
    } catch (err: Exception) {
        emptyList<String>()
    }

    val titleTerms = analyzer.tokenize("title", html.select("title").text())

    val anchorTerms = analyzer.tokenize("body", html.select("a").text())
    val tableTerms = analyzer.tokenize("body", html.select("table").text())

    val features = pmake {
        set("jsoup_error", false)
        putIfNotNull("byte_length", raw_text?.length)
        set("numTerms", allTerms.size)
        set("numVisTerms", visTerms.size)
        set("numTitleTerms", titleTerms.size)
        set("fracVisibleText", safeDiv(visTerms.size, allTerms.size))
        set("fracAnchorText", safeDiv(anchorTerms.size, allTerms.size))
        set("fracTableText", safeDiv(tableTerms.size, allTerms.size))
        set("avgTermLength", visTerms.map { it.length }.mean())
        set("avgAnchorTermLength", anchorTerms.map { it.length }.mean())
        set("urlDepth", url.count { it == '/' })
        set("urlSize", url.length)
        set("entropy", allTerms.computeEntropy())
    }

    listOf(
            Pair("inquery", inqueryStop),
            Pair("web100", web100Stop),
            Pair("web1k", web1kStop)).forEach { (name, list) ->
        val found = hashSetOf<String>()
        val stop = allTerms.sumBy {
            if (list.contains(it)) {
                found.add(it)
                1
            } else 0
        }
        features.set("$name.fracStops", safeDiv(stop, allTerms.size))
        features.set("$name.stopCover", safeDiv(found.size, list.size))
    }


    return features
}

object ExtractHTMLFeatures {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)!!
        val ids = File(argp.get("needed", "wt10g.needed.ids")).readLines().toHashSet()
        val urls = File(argp.get("urls", "html_raw/wt10g.urls.tsv.gz"))

        val input = File(argp.get("input", "html_raw/wt10g.sample.jsonl.gz"))
        val output = File(argp.get("output", "html_raw/wt10g.features.jsonl.gz"))
        val logger = FileLogger(argp.get("logger", "${output.absolutePath}.log"))!!

        val urlById = HashMap<String, String>()
        urls.smartDoLines { line ->
            val cols = line.split("\t")
            val id = cols[0]
            if (ids.contains(id)) {
                val url = cols[2]
                urlById[id] = url
            }
        }

        output.smartPrint { writer ->
            input.smartDoLines(true, total=ids.size.toLong()) { line ->
                val lineP = Parameters.parseStringOrDie(line)
                val id = lineP.getStr("id")
                val content = lineP.getStr("content")
                val url = urlById[id] ?: ""
                //println("$id ${content.length}")
                try {
                    val featureP = computeHTMLStaticFeatures(logger, raw_text=content, url=url)
                    val outP = pmake { set("id", id); set("features", featureP) }
                    writer.println(outP)
                    //println(outP)
                } catch (e: Exception) {
                    logger.log(Level.WARNING, "exception in $id", e)
                    println("exception in $id")
                    //throw e
                }
            }
        }
    }
}

object JsoupBug {
    @JvmStatic fun main(args: Array<String>) {
        Jsoup.clean("<a href>clean bomb</a>", Whitelist.simpleText())
    }
}
