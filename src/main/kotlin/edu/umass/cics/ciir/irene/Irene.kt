package edu.umass.cics.ciir.irene

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper
import org.apache.lucene.benchmark.byTask.feeds.DocData
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException
import org.apache.lucene.benchmark.byTask.feeds.TrecContentSource
import org.apache.lucene.benchmark.byTask.utils.Config
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.*
import org.apache.lucene.search.*
import java.io.Closeable
import java.io.File
import java.util.*
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author jfoley.
 */

data class CountStats(val cf: Long, val df: Long, val cl: Long, val dc: Long) {
    fun avgDL() = cl.toDouble() / dc.toDouble();
    fun countProbability() = cf.toDouble() / cl.toDouble()
    fun nonzeroCountProbability() = Math.max(0.5,cf.toDouble()) / cl.toDouble()
    fun binaryProbability() = df.toDouble() / dc.toDouble()
}

class IndexParams {
    var defaultField = "body"
    private var defaultAnalyzer = IreneEnglishAnalyzer()
    private var perFieldAnalyzers = HashMap<String, Analyzer>()
    var directory: edu.umass.cics.ciir.irene.RefCountedIO? = null
    var openMode: IndexWriterConfig.OpenMode? = null
    var idFieldName = "id"

    fun withAnalyzer(field: String, analyzer: Analyzer) {
        perFieldAnalyzers.put(field, analyzer)
    }
    fun inMemory() {
        directory = edu.umass.cics.ciir.irene.RefCountedIO(MemoryIO())
        create()
    }
    fun withPath(fp: File) {
        directory = edu.umass.cics.ciir.irene.RefCountedIO(DiskIO.open(fp.toPath()))
    }
    fun create() {
        openMode = IndexWriterConfig.OpenMode.CREATE
    }
    fun append() {
        openMode = IndexWriterConfig.OpenMode.CREATE_OR_APPEND
    }
    val analyzer: Analyzer
            get() = if (perFieldAnalyzers.isEmpty()) {
                defaultAnalyzer
            } else {
                PerFieldAnalyzerWrapper(defaultAnalyzer, perFieldAnalyzers)
            }
}

class IreneIndexer(val params: IndexParams) : Closeable {
    companion object {
        fun build(setup: IndexParams.() -> Unit): IreneIndexer {
            return IreneIndexer(IndexParams().apply(setup))
        }
    }
    val processed = AtomicLong(0)
    val dest = params.directory!!
    val writer = IndexWriter(dest.use(), IndexWriterConfig(params.analyzer).apply {
        similarity = TrueLengthNorm()
        openMode = params.openMode
    })
    override fun close() {
        writer.close()
    }
    fun commit() {
        writer.commit()
    }
    fun push(vararg doc: IndexableField) {
        writer.addDocument(doc.toList())
        processed.incrementAndGet()
    }
    fun push(doc: Iterable<IndexableField>) {
        writer.addDocument(doc)
        processed.incrementAndGet()
    }
    fun open() = IreneIndex(dest, params)
}

class IreneIndex(val io: RefCountedIO, params: IndexParams) : Closeable {
    constructor(params: IndexParams) : this(params.directory!!, params)
    val defaultField = params.defaultField
    val idFieldName = params.idFieldName
    val reader = DirectoryReader.open(io.open().use())
    val searcher = IndexSearcher(reader, ForkJoinPool.commonPool())
    val analyzer = params.analyzer
    val language = IreneQueryLanguage()

    override fun close() {
        reader.close()
        io.close()
    }

    fun getField(doc: Int, name: String): IndexableField? = searcher.doc(doc, setOf(name))?.getField(name)
    fun getDocumentName(doc: Int): String? {
        return getField(doc, idFieldName)?.stringValue()
    }
    fun document(doc: Int): LDoc? {
        return lucene_try { searcher.doc(doc) }
    }
    fun documentById(id: String): Int? {
        val q = BooleanQuery.Builder().add(TermQuery(Term(idFieldName, id)), BooleanClause.Occur.MUST).build()!!

        return lucene_try {
            val results = searcher.search(q, 10)?.scoreDocs
            if (results == null || results.size == 0) return -1
            // TODO complain about dupes?
            return results[0].doc
        }
    }
    fun terms(doc: Int, field: String): List<String> {
        val text = getField(doc, field)?.stringValue()
        if (text == null) return emptyList()
        return analyzer.tokenize(field, text)
    }
    val cache: Cache<Term, CountStats> = Caffeine.newBuilder().maximumSize(100_000).build()

    fun getAverageDL(field: String): Double {
        val fieldStats = searcher.collectionStatistics(field)
        return fieldStats.sumTotalTermFreq().toDouble() / fieldStats.docCount().toDouble()
    }
    fun getStats(text: String, field: String = defaultField): CountStats? = getStats(Term(field, text))
    private fun getStatsDirect(term: Term): CountStats? {
        val cstats = searcher.collectionStatistics(term.field())
        val ctx = TermContext.build(searcher.topReaderContext, term) ?: return null
        val termStats = searcher.termStatistics(term, ctx) ?: return null
        return CountStats(termStats.docFreq(), termStats.totalTermFreq(), cstats.sumTotalTermFreq(), cstats.docCount())
    }
    fun getStats(term: Term): CountStats? {
        return cache.get(term, this::getStatsDirect)
    }

    fun search(q: QExpr, n: Int): TopDocs {
        return searcher.search(IreneQueryModel(this, this.language, q), n)!!
    }

    fun tokenize(text: String, field: String=defaultField) = this.analyzer.tokenize(field, text)

}

fun main(args: Array<String>) {
    val tcs = TrecContentSource()
    tcs.config = Config(Properties().apply {
        setProperty("tests.verbose", "false")
        setProperty("content.source.excludeIteration", "true")
        setProperty("content.source.forever", "false")
        setProperty("docs.dir", "/media/jfoley/flash/raw/robust04/data/")
    })

    IreneIndexer.build {
        withPath(File("robust.irene2"))
        create()
    }.use { writer ->
        while(true) {
            var doc = DocData()
            try {
                doc = tcs.getNextDocData(doc)
            } catch (e : NoMoreDataException) {
                break
            } catch (e : Throwable) {
                e.printStackTrace(System.err)
            }

            val fields = arrayListOf<IndexableField>()
            fields.add(StringField("id", doc.name, Field.Store.YES))
            if (doc.title != null) {
                fields.add(TextField("title", doc.title, Field.Store.YES))
            }
            fields.add(TextField("body", doc.body, Field.Store.YES))
            if (doc.date != null) {
                fields.add(StringField("date", doc.date, Field.Store.YES))
            }
            if(doc.props.size > 0) {
                println(doc.props)
            }
            writer.push(fields)
        }
    }
}

