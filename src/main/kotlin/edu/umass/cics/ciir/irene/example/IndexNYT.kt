package edu.umass.cics.ciir.irene.example

/**
 *
 * @author jfoley.
 */

import edu.umass.cics.ciir.chai.CountingDebouncer
import edu.umass.cics.ciir.chai.Debouncer
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.reader
import gnu.trove.map.hash.TObjectFloatHashMap
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.*
import org.apache.lucene.search.ScoreDoc
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.parser.Parser
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.*
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import java.util.zip.GZIPInputStream
import javax.xml.parsers.DocumentBuilderFactory
import kotlin.coroutines.experimental.buildSequence
import kotlin.streams.asStream

object NYT {
    var EST = TimeZone.getTimeZone("EST")

    val MinYear = 1987
    val MaxYear = 2007
    val MaxDate = LocalDate.of(2007, 6, 19)

    val NumDocs = 1855490
    val CorpusStart: Long = 536457600
    val CorpusEnd: Long = 1182211200

    fun getNormCorpusTime(epochSec: Long): Float {
        return (epochSec - CorpusStart).toFloat() / (CorpusEnd - CorpusStart)
    }

    fun getIndex(): IreneIndex = TODO("")

    fun getIndexParams(path: String) = IndexParams().apply {
        withPath(File(path))
        withAnalyzer("classifier", WhitespaceAnalyzer())
    }
}

object TitlePatterns {
    fun isEarningsReport(title: String): Boolean {
        return title.toLowerCase().contains("reports earnings for")
    }

    fun isObituary(title: String): Boolean {
        return title.toLowerCase().startsWith("paid notice:")
    }

    fun isNoHeadline(title: String): Boolean {
        return title.equals("No Headline", ignoreCase = true)
    }

    fun isCorrection(title: String): Boolean {
        val ltitle = title.toLowerCase()
        return ltitle.startsWith("correction") || ltitle.startsWith("a correction") || ltitle.endsWith("correction") || ltitle.endsWith("corrections")
    }
}


fun Element.selectSingleChild(tag: String): Element? = this.children().find { it.tagName() == tag }
fun Element.innerText(): String = this.text()

/**
 * @author jfoley.
 */
class NYTDoc {
    val id: String
    lateinit var classifier: String
    private var wasCorrected = false
    private var lead: String? = null
    private var text: String? = null
    var pageNum: Int = -1
    lateinit var section: String
    var headline: String? = null
    lateinit var desk: String
    private var byline: String? = null
    var title: String? = null
    lateinit var body: String
    private val date: LocalDateTime
    private val epochSec: Long
    private var score = 0.0f

    val dateStr: String
        get() = String.format("%04d-%02d-%02d", date.year, date.monthValue, date.dayOfMonth)

    constructor(fields: Document) {
        this.id = fields["id"] ?: error("Must have id stored.")
        this.title = fields["title"] ?: ""
        this.headline = fields["headline"] ?: ""
        this.body = fields.get("body")
        this.epochSec = fields.getField("storedTimeStamp").numericValue().toLong()
        this.date = LocalDateTime.ofEpochSecond(epochSec, 0, ZoneOffset.UTC)
    }

    constructor(xml: Element) {
        val head = xml.selectFirst("head")
        if (head == null) {
            println(xml.html())
            error("Must have <head>")
        }

        val xtitle = head.selectSingleChild("title")
        if (xtitle != null) {
            this.title = xtitle.innerText()
        }

        val docdata = head.selectSingleChild("docdata") ?: error("Must have <head>/<docdata>")
        this.id = docdata.selectSingleChild("doc-id")?.attr("id-string") ?: error("Must have <docdata>/<doc-id>")

        val classifiers = ArrayList<String>()
        for (xc in docdata.select("classifier")) {
            classifiers.add(xc.innerText())
        }
        classifier = classifiers.joinToString(separator = "\n")

        val bodyTag = xml.selectSingleChild("body")

        if (bodyTag != null) {
            headline = bodyTag.selectSingleChild("hedline")?.innerText() ?: ""
            if (TitlePatterns.isNoHeadline(headline!!)) {
                headline = ""
            }


            val bylines = bodyTag.select("byline")
            if (bylines.isEmpty()) {
                byline = ""
            } else {
                for (xNode in bylines) {
                    if (xNode.attr("class").toLowerCase() == ("normalized_byline")) {
                        byline = xNode.innerText()
                    }
                }
                // fall-back to "any" byline.
                if (byline == null) {
                    bylines[0].innerText()
                }
            }

            val blocks = bodyTag.select("block")
            for (block in blocks) {
                val blockClass = block.attr("class")
                when (blockClass) {
                    "lead_paragraph" -> lead = joinParagraphs(block)
                    "online_lead_paragraph" -> {
                        if (lead == null) { // only if not another
                            lead = joinParagraphs(block)
                        }
                    }
                    "full_text" -> text = joinParagraphs(block)
                    "correction_text" -> wasCorrected = true
                    else -> throw RuntimeException(id + "\t" + blockClass)
                }
            }
        }

        val meta = HashMap<String, String>()
        for (xmeta in head.select("meta")) {
            meta.put(xmeta.attr("name"), xmeta.attr("content"))
        }
        if (meta.containsKey("correction_date")) {
            wasCorrected = true
        }

        val year = Integer.parseInt(meta.getOrDefault("publication_year", "0"))
        val day = Integer.parseInt(meta.getOrDefault("publication_day_of_month", "1"))
        val month = Integer.parseInt(meta.getOrDefault("publication_month", "1"))

        this.pageNum = Integer.parseInt(meta.getOrDefault("print_page_number", "10000"))
        this.section = meta.getOrDefault("print_section", "NO_SECTION_VALUE")
        desk = meta.getOrDefault("dsk", "NO_DESK_VALUE")

        // ASSUME 8AM EST
        date = LocalDateTime.of(year, month, day, 8, 0)
        epochSec = date.atZone(NYT.EST.toZoneId()).toInstant().epochSecond
    }

    fun extractStaticFeatures(): TObjectFloatHashMap<String> {
        val staticFeatures = TObjectFloatHashMap<String>()

        val title = this.title ?: ""
        staticFeatures.put("title.length", title.length.toFloat())
        //staticFeatures.put("title.df", NYT.getTitleDF(title))
        staticFeatures.put("title.earnings", ofBool(TitlePatterns.isEarningsReport(title)))
        staticFeatures.put("title.obit", ofBool(TitlePatterns.isObituary(title)))
        staticFeatures.put("title.correction", ofBool(TitlePatterns.isCorrection(title)))
        staticFeatures.put("body.length", body.length.toFloat())
        staticFeatures.put("date." + date.dayOfWeek.toString(), ofBool(true))
        staticFeatures.put("date.continuous", NYT.getNormCorpusTime(epochSec))

        return staticFeatures
    }

    fun toLuceneDoc(): LDoc {
        val ldoc = LDoc()
        ldoc.add(StringField("id", id, Field.Store.YES))
        if (title != null) ldoc.add(TextField("title", title!!, Field.Store.YES))
        if (headline != null) ldoc.add(TextField("headline", headline!!, Field.Store.YES))
        ldoc.add(TextField("classifier", classifier, Field.Store.YES))
        if (lead != null) ldoc.add(TextField("lead", lead!!, Field.Store.YES))
        if (text != null) ldoc.add(TextField("text", text!!, Field.Store.YES))
        if (byline != null) ldoc.add(TextField("byline", byline!!, Field.Store.YES))
        ldoc.add(StringField("desk", desk, Field.Store.YES))

        ldoc.add(IntPoint("day", date.dayOfMonth))
        ldoc.add(IntPoint("month", date.monthValue))
        ldoc.add(IntPoint("year", date.dayOfYear))

        // store raw timestamp for parsing it out:
        ldoc.add(StoredField("storedTimeStamp", date.atZone(NYT.EST.toZoneId()).toEpochSecond()))

        ldoc.add(StringField("section", section, Field.Store.YES))
        ldoc.add(IntPoint("pageNum", pageNum))
        ldoc.add(BoolField("corrected", wasCorrected, true))
        return ldoc
    }

    object FindDateInfo {
        @JvmStatic
        fun main(args: Array<String>) {
            // find date-info
            var min = java.lang.Long.MAX_VALUE
            var max: Long = 0
            var count: Long = 0
            try {
                NYT.getIndex().use({ index ->
                    val msg = Debouncer()

                    for (id in (0 until index.totalDocuments)) {
                        count++
                        if (msg.ready()) {
                            System.out.println(msg.estimate(count, NYT.NumDocs.toLong()))
                        }
                        try {
                            val unixSecond = index.reader.document(id, setOf("storedTimeStamp")).getField("storedTimeStamp").numericValue().toLong()
                            if (unixSecond < min) {
                                min = unixSecond
                            }
                            if (unixSecond > max) {
                                max = unixSecond
                            }
                        } catch (e: IOException) {
                            e.printStackTrace()
                        }

                    }
                })
            } catch (e: IOException) {
                e.printStackTrace()
            }

            println(min)
            println(max)
        }
    }

    companion object {

        private fun joinParagraphs(block: Element): String {
            val text = StringBuilder()
            for (paragraph in block.select("p")) {
                text.append(paragraph.innerText()).append('\n')
            }
            return text.toString()
        }

        fun fetch(src: IreneIndex, base: Int): NYTDoc? {
            val fields = src.document(base) ?: return null
            return NYTDoc(fields)
        }

        fun fetch(src: IreneIndex, fromSearch: ScoreDoc): NYTDoc? {
            val fields = src.document(fromSearch.doc) ?: return null
            val doc = NYTDoc(fields)
            doc.score = fromSearch.score
            return doc
        }

        fun ofBool(feature: Boolean): Float {
            return if (feature) 1f else 0f
        }
    }
}

/**
 * For when you want to read ''amt'' bytes from an [InputStream], no matter how many syscalls it takes.
 * @param is the input stream to read from.
 * @param requested the number of bytes to read.
 * @return a [ByteArray] filled with the next amt bytes from the input stream.
 * @throws EOFException if done
 * @throws IOException if the stream complains
 */
@Throws(IOException::class)
fun readBytes(`is`: InputStream, requested: Int): ByteArray {
    var amt = requested
    if (amt == 0) {
        return ByteArray(0)
    }
    val buf = ByteArray(amt)

    // Begin I/O loop:
    var off = 0
    while (true) {
        assert(off + amt <= buf.size)
        val read = `is`.read(buf, off, amt)
        if (read < -1) {
            throw EOFException()
        }
        if (read == amt) break

        // Ugh; try again
        off += read
        amt -= read
    }
    return buf
}

data class TarEntryData(val name: String, private val content: ByteArray) {
    val text: String by lazy {
        if (name.endsWith(".gz")) {
            GZIPInputStream(ByteArrayInputStream(content)).reader().readText()
        } else {
            String(content, Charsets.UTF_8)
        }
    }
}
fun tarEntries(fp: File): Sequence<TarEntryData> = buildSequence {
    TarArchiveInputStream(StreamCreator.openInputStream(fp)).use { tarStream ->
        while(true) {
            val current = tarStream.nextTarEntry ?: break
            if (current.size == 0L) continue
            val streamSize = current.size.toInt()
            val content = readBytes(tarStream, streamSize)
            yield(TarEntryData(current.name, content))
        }
    }
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)

    val factory = DocumentBuilderFactory.newInstance()
    factory.isValidating = false
    factory.isExpandEntityReferences = false

    val params = NYT.getIndexParams("/media/jfoley/flash/nytxml.irene2").apply {
        create()
    }
    val dir = File(argp.get("input", "/media/jfoley/flash/raw/nyt/"))
    IreneIndexer(params).use { indexer ->
        val msg = CountingDebouncer(NYT.NumDocs.toLong())
        for (i in NYT.MinYear..NYT.MaxYear) {
            tarEntries(File(dir, "nyt-$i.tar")).asStream().parallel().forEach { entry ->
                val text = entry.text.replace("<!DOCTYPE nitf SYSTEM \"http://www.nitf.org/IPTC/NITF/3.3/specification/dtd/nitf-3-3.dtd\">", "\n")
                try {
                    val xsoup = Jsoup.parse(text, "http://www.nyt.com/", Parser.xmlParser())
                    val doc = NYTDoc(xsoup)

                    indexer.push(doc.toLuceneDoc())
                    msg.incr()?.let { upd ->
                        println(upd)
                    }
                } catch (e: Exception) {
                    System.err.println(entry.name+": "+e.message)
                }
            }
        }
    } // close indexer
}