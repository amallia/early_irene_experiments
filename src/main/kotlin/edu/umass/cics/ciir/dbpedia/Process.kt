package edu.umass.cics.ciir.dbpedia

import edu.umass.cics.ciir.Debouncer
import edu.umass.cics.ciir.sprf.IRDataset
import edu.umass.cics.ciir.sprf.pmake
import edu.umass.cics.ciir.sprf.printer
import gnu.trove.list.array.TIntArrayList
import gnu.trove.map.hash.TIntObjectHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.*

val dir = when(IRDataset.host) {
    "gob" -> "/media/jfoley/flash/dbpedia-2016-10"
    "oakey" -> "/mnt/scratch/jfoley/dbpedia-2016-10"
    else -> error("Death!")
}

// Roughly 5 million articles in the "long_abstracts" file.
val PagePrefix = "http://dbpedia.org/resource/"
val QPagePrefix = "http://wikidata.dbpedia.org/resource/"

// Labels
val Labels = "labels_en.ttl.bz2"
// Wikidata Qnnnnnnn labels.
val QLabels = "labels_wkd_uris_en.ttl.bz2"

// Article -> Anchor Text
val AnchorText = "anchor_text_en.ttl.bz2"
// Article -> Cat
val ArticleCategories = "article_categories_en.ttl.bz2"
// Cat -> Labels
val CategoryLabels = "category_labels_en.ttl.bz2"
// Cat->Cat
val CategoryGraph = "skos_categories_en.ttl.bz2"
// Cat -> Article
val CategoryArticles = "topical_concepts_en.ttl.bz2"


// Article -> Link
val ArticleCitations = "citation_links_en.ttl.bz2"
// Link -> Props
val CitationData = "citation_data_en.ttl.bz2"

// Disambiguations
val Disambiguations = "disambiguations_en.ttl.bz2"
// Redirects
val Redirects = "transitive_redirects_en.ttl.bz2"

// Long Abstracts
val LongAbstracts = "long_abstracts_en.ttl.bz2"
// Full Text
val FullTextNIF = "nif_context_en.ttl.bz2"
// Page Links
val PageLinks = "page_links_en.ttl.bz2"
val PersonData = "persondata_en.ttl.bz2"

class PeekReader(val rdr: Reader) {
    var next: Int = rdr.read()
    fun eof(): Boolean = next == -1
    fun peek(): Int = next
    fun getc(): Int {
        val ret = next
        if (next != -1) {
            next = rdr.read()
        }
        return ret
    }
    fun deleteUntil(x: Char) {
        val term = x.toInt()
        while(next != -1 && next != term) {
            next = rdr.read()
        }
        val success = getc() == term
        assert(success)
    }

    val URLEnd = '>'.toInt()
    val URLStart = '<'.toInt()
    fun readURL(): String? {
        val sb = StringBuilder()
        val correctCall = getc() == URLStart
        assert(correctCall)
        while (next != URLEnd) {
            sb.append(next.toChar())
            next = rdr.read()
            if (next == -1) return null
            if (next == URLEnd) break
        }
        val success = getc() == URLEnd
        assert(success)
        return sb.toString()
    }
    fun consume(c : Char) {
        val next = getc()
        if (next == -1) error("EOF instead of expected |$c|.")
        if (next.toChar() != c) {
            error("${next.toChar()} instead of expected |$c|.")
        }
        return
    }

    fun readString(line: Int): Pair<String, String?> {
        val sb = StringBuilder()
        consume('"')
        while (true) {
            val cx = peek()
            if (cx == -1) {
                error("$line: EOF in string.")
            }
            val c = cx.toChar()
            if (c == '"') {
                consume('"')
                break
            }
            else if (c == '\\') {
                consume('\\')

                // Then read the next "escaped" character.
                val escaped = getc()
                if (escaped == -1) error("$line: EOF in escape sequence.")
                when (escaped.toChar()) {
                    '\\' -> sb.append('\\')
                    '\"' -> sb.append('"')
                    '\'' -> sb.append('\'')
                    'n' -> sb.append('\n')
                    'r' -> sb.append('\r')
                    't' -> sb.append('\t')
                }
            } else sb.append(getc().toChar())
        }
        if (peek() == '@'.toInt()) {
            // consume language code:
            consume('@')
            consume('e')
            consume('n')
        }
        val kind =
                if (peek() == '^'.toInt()) {
                    consume('^')
                    consume('^')
                    assert(peek() == '<'.toInt())
                    readURL() ?: error("$line: EOF in type string")
                } else null
        return Pair(sb.toString(), kind)
    }
}

fun forEachTTLRecord(input: Reader, handle: (List<String>)->Unit) {
    val msg = Debouncer(5000)
    var processed = 0
    val reader = PeekReader(input)
    val record = arrayListOf<String>()
    while(!reader.eof()) {
        val next = reader.peek()
        if (next == -1) break
        val nc = next.toChar()
        if (nc == '#') {
            reader.deleteUntil('\n');
        } else if (nc == '<') {
            val maybeURL = reader.readURL()
                    ?: error("$processed: error in parsing URL, $record, ${reader.peek()}")
            record.add(maybeURL)
        } else if (nc == '"') {
            val (value, _) = reader.readString(processed)
            record.add(value)
        } else if (nc == '.') {
            // end record
            handle(record)
            record.clear()
            processed++
            if (msg.ready()) {
                println("Processed $processed TTL records in ${msg.spentTimePretty}.")
            }
            reader.deleteUntil('\n')
        } else if (Character.isWhitespace(nc)) {
            reader.getc()
            continue;
        } else error("Unexpected TTL delimiter: ``$nc'' ``$next''")
    }
}

fun forEachTTLRecord(path: String, handle: (List<String>)->Unit) {
    forEachTTLRecord(StreamCreator.openInputStream(path).bufferedReader(), handle)
}

fun File.smartMkdir(): Boolean {
    if (this.exists() && this.isDirectory) return true
    return this.mkdir()
}
fun File.ensureParentDirectories(): Boolean {
    if (this.exists() && this.isDirectory) return true
    if (this.smartMkdir()) return true
    if (this.parentFile != null) {
        return this.parentFile.ensureParentDirectories() && this.smartMkdir()
    }
    return false
}

class ShardWriters(val outDir: File, val shards: Int = 10, name: String) : Closeable {
    init {
        println("PWD: "+File(".").absolutePath)
        outDir.ensureParentDirectories()
    }
    val outFiles: List<PrintWriter> = (0 until shards).map {
        val shardDir = File(outDir, "shard$it")
        if (!shardDir.ensureParentDirectories()) error("Cannot create $shardDir")
        val output = File(shardDir, name)
        println("Opening $output")
        PrintWriter(StreamCreator.openOutputStream(output).bufferedWriter())
    }

    operator fun get(i: Int): PrintWriter = outFiles[i]
    fun <T> hashed(obj: T): PrintWriter =
            outFiles[Math.abs(obj?.hashCode() ?: 0) % shards]

    override fun close() {
        val errs = outFiles.map {
            try {
                it.close()
                null
            } catch (throwable: Throwable) {
                throwable
            }
        }.filterNotNull()

        if (errs.isNotEmpty())
            throw IOException(errs.joinToString(separator = "\n\n"))
    }

}

fun forEachTTLRecord(inName: String, sample: Boolean, mapper: (List<String>) -> Unit) {
    val path = if (sample) {
        "$dir/sample/$inName.sample"
    } else {
        "$dir/$inName"
    }
    forEachTTLRecord(path, mapper)
}
fun mapFileToShards(inName: String, sample: Boolean, shards: Int, outName: String, mapper: (List<String>, ShardWriters)->Unit) {
    ShardWriters(File("dbps"), shards, outName).use { writers ->

        val path = if (sample) {
            "$dir/sample/$inName.sample"
        } else {
            "$dir/$inName"
        }
        forEachTTLRecord(path) { record -> mapper(record, writers) }
    }
}

object ProcessAndShardFullText {
    fun isWikidata(url: String): Boolean = url.startsWith(QPagePrefix)
    fun isResource(url: String): Boolean = url.startsWith(PagePrefix)
    fun getKey(url: String): String {
        val start = if (isResource(url)) {
            PagePrefix.length
        } else if (isWikidata(url)) {
            QPagePrefix.length
        } else {
            0
        }
        val beforeQueryParams = url.lastIndexOf("?")
        if (beforeQueryParams >= 0) {
            return url.substring(start, beforeQueryParams)
        }
        return url.substring(start)
    }
    fun lastSlash(url: String): String {
        var beforeQueryParams = url.lastIndexOf("?")
        var lastSlash = url.lastIndexOf('/')
        if (lastSlash < 0) {
            lastSlash = 0
        } else {
            lastSlash += 1
        }
        if (beforeQueryParams < 0) beforeQueryParams = url.length
        return url.substring(lastSlash, beforeQueryParams)
    }
    fun simple(record: List<String>, writers: ShardWriters) {
        val page = getKey(record[0])
        val text = record[2]

        writers.hashed(page).println(pmake {
            set("id", page)
            set("text", text)
        })
    }
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val shards = argp.get("shards", 20)
        val sample = argp.get("sample", true)

        // Page-labels
        val pageLabels = HashMap<String,String>()
        val qPageLabels = TIntObjectHashMap<String>()
        forEachTTLRecord(Labels, sample) { record ->
            val page = getKey(record[0])
            val text = record[2]
            if (page != text) {
                pageLabels[page] = text
            }
        }
        forEachTTLRecord(QLabels, sample) { record ->
            val page = getKey(record[0])
            assert(page[0] == 'Q')
            val wkd_id = page.substring(1).toInt()
            val text = record[2]
            if (page != text) {
                qPageLabels.put(wkd_id, text)
            }
        }

        // Person-data
        mapFileToShards(PersonData, sample, shards, "person_fields.jsonl.gz") { record, writers ->
            if (!record[1].endsWith("22-rdf-syntax-ns#type")) {
                val page = getKey(record[0])
                val field = lastSlash(record[1])
                val text = if (isResource(record[2])) {
                    val key = getKey(record[2])
                    pageLabels[key] ?: key
                } else if (isWikidata(record[2])) {
                    val key = getKey(record[2])
                    qPageLabels.get(key.substring(1).toInt()) ?: null
                } else {
                    record[2]
                }

                if (text != null) {
                    writers.hashed(page).println(pmake {
                        set("id", page)
                        set(field, text)
                    })
                }
            }
        }
        pageLabels.clear()
        qPageLabels.clear()

        // Redirects are basically mis-spellings, but also alternative titles.
        mapFileToShards(Redirects, sample, shards, "redirects.jsonl.gz") { record, writers ->
            val from = getKey(record[0])
            val page = getKey(record[2])
            writers.hashed(page).println(pmake {
                set("id", page)
                set("text", from) // tokenize later.
            })
        }

        val citationTitles = HashMap<String,String>()
        forEachTTLRecord(CitationData, sample) { record ->
            if (record[1].endsWith("/title")) {
                val citation = record[0]
                val text = record[2]
                citationTitles[citation] = text
            }
        }
        mapFileToShards(ArticleCitations, sample, shards, "article_cites.jsonl.gz") { record, writers ->
            val citation = record[0]
            val page = getKey(record[2])
            val title = citationTitles[citation]
            if (title != null) {
                writers.hashed(page).println(pmake {
                    set("id", page)
                    set("text", title)
                })
            }
        }
        // free memory.
        citationTitles.clear()


        mapFileToShards(FullTextNIF, sample, shards, "full_text.jsonl.gz") { record, writers ->
            if (record[1].endsWith("isString")) {
                simple(record, writers)
            }
        }

        mapFileToShards(AnchorText, sample, shards, "anchor_texts.jsonl.gz") { record, writers -> simple(record, writers) }
        mapFileToShards(ArticleCategories, sample, shards, "article_categories.jsonl.gz") { record, writers -> simple(record, writers) }


    }
}

fun <T> Parameters.setIfNotEmpty(key: String, x: Collection<T>) {
    if (x.isNotEmpty()) {
        this.set(key, x.toList())
    }
}

class CategoryInfo(val id: Int, val name: String, var prefLabel: String? = null) {
    val children = TIntArrayList()
    val parents = TIntArrayList()
    val related = TIntArrayList()
    var pages = ArrayList<String>()
    var labels = ArrayList<String>()

    fun toJSON(): Parameters = pmake {
        set("id", id)
        set("name", name)
        putIfNotNull("prefLabel", prefLabel)
        setIfNotEmpty("labels", labels.toMutableSet().apply {
            val pl = prefLabel
            if (pl!=null) { add(pl) }
        }.toList())
        setIfNotEmpty("pages", pages)
        setIfNotEmpty("related", related.toArray().toList())
        setIfNotEmpty("parents", parents.toArray().toList())
        setIfNotEmpty("children", children.toArray().toList())
    }
}
class CategoryGraphBuilder {
    val nodes = HashMap<String, CategoryInfo>()
    operator fun get(id: String): CategoryInfo = nodes.computeIfAbsent(id, { CategoryInfo(nodes.size, it) })
}

object CategoryGraphAnalysis {
    val CategoryPrefix = "http://dbpedia.org/resource/Category:"
    fun category(x: String): String? {
        if (x.startsWith(CategoryPrefix)) {
            return x.substring(CategoryPrefix.length)
        } else return null
    }
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val sample = argp.get("sample", true)

        val graph = CategoryGraphBuilder()
        forEachTTLRecord(CategoryLabels, sample) { record ->
            val name = category(record[0])
            if (name != null) {
                graph[name].labels.add(record[2])
            }
        }
        forEachTTLRecord(CategoryArticles, sample) { record ->
            if (record[1].endsWith("/terms/subject")) {
                val name = category(record[0])
                if (name != null) {
                    val page = ProcessAndShardFullText.getKey(record[2])
                    graph[name].pages.add(page)
                }
            }
        }
        forEachTTLRecord(CategoryGraph, sample) { record ->
            val name = category(record[0])!!
            if (record[1].endsWith("22-rdf-syntax-ns#type")) {
                // skip the "isa Concept label"
            } else if (record[1].endsWith("#prefLabel")) {
                graph[name].prefLabel = record[2]
            } else if (record[1].endsWith("/core#broader")) {
                val name2 = category(record[2])!!
                val parent = graph[name]
                val child = graph[name2]
                parent.children.add(child.id)
                child.parents.add(parent.id)
            } else if (record[1].endsWith("/core#related")) {
                val name2 = category(record[2])!!
                val lhs = graph[name]
                val rhs = graph[name2]
                lhs.related.add(rhs.id)
                rhs.related.add(lhs.id)
            } else error("Unknown relation: ${record[1]}")
        }

        println(graph.nodes.size)
        StreamCreator.openOutputStream("cgraph.jsonl.gz").printer().use { out ->
            graph.nodes.values.forEach { info ->
                out.println(info.toJSON())
            }
        }
    }
}

/**
 *
 * @author jfoley.
 */
fun main(args: Array<String>) {

    val relFreqs = TObjectIntHashMap<String>()

   StreamCreator.openInputStream("$dir/sample/long_abstracts_en.ttl.bz2.sample").bufferedReader().use { reader ->
       forEachTTLRecord(reader) { record ->
           if (!record[0].startsWith("http://wikidata")) {
               println(record)
           }
           relFreqs.adjustOrPutValue(record[1], 1, 1)
       }
   }
    println(relFreqs)

    val categories = "article_categories_en.ttl.bz2"
    StreamCreator.openInputStream("$dir/sample/$categories.sample").reader().use { input ->
        forEachTTLRecord(input.buffered()) { record ->
            //println(record)
            relFreqs.adjustOrPutValue(record[1], 1, 1)
        }
    }
    println(relFreqs)
}
