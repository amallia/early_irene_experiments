package edu.umass.cics.ciir.irene

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import edu.umass.cics.ciir.iltr.RREnv
import edu.umass.cics.ciir.irene.indexing.LDocBuilder
import edu.umass.cics.ciir.irene.scoring.IreneQueryModel
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
import org.lemurproject.galago.utility.Parameters
import java.io.Closeable
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinTask
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * @author jfoley.
 */

fun LDoc.toParameters(): Parameters {
    val output = Parameters.create()
    fields.forEach { field ->
        val name = field.name()!!
        output.putIfNotNull(name, field.stringValue())
        output.putIfNotNull(name, field.numericValue())
    }
    return output
}

class IndexParams {
    var defaultField = "body"
    var defaultAnalyzer: Analyzer = IreneEnglishAnalyzer()
    private var perFieldAnalyzers = HashMap<String, Analyzer>()
    var directory: RefCountedIO? = null
    var openMode: IndexWriterConfig.OpenMode? = null
    var idFieldName = "id"

    fun withAnalyzer(field: String, analyzer: Analyzer) {
        perFieldAnalyzers.put(field, analyzer)
    }
    fun inMemory() {
        directory = RefCountedIO(MemoryIO())
        create()
    }
    fun withPath(fp: File) {
        directory = RefCountedIO(DiskIO.open(fp.toPath()))
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
    fun push(vararg doc: IndexableField): Long {
        writer.addDocument(doc.toList())
        return processed.incrementAndGet()
    }
    fun push(doc: Iterable<IndexableField>): Long {
        writer.addDocument(doc)
        return processed.incrementAndGet()
    }
    fun open() = IreneIndex(dest, params)

    fun doc(fn: LDocBuilder.()->Unit) {
        val doc = LDocBuilder(params)
        fn(doc)
        push(doc.finish())
    }
}

interface IIndex : Closeable {
    val tokenizer: GenericTokenizer
    val defaultField: String
    val totalDocuments: Int
    fun getRREnv(): RREnv
    fun fieldStats(field: String): CountStats?
    fun getStats(expr: QExpr): CountStats
    fun getStats(text: String, field: String = defaultField): CountStats = getStats(Term(field, text))
    fun getStats(term: Term): CountStats
    fun tokenize(text: String, field: String=defaultField) = tokenizer.tokenize(text, field)
    fun toTextExprs(text: String, field: String = defaultField): List<TextExpr> = tokenize(text, field).map { TextExpr(it, field) }
    fun search(q: QExpr, n: Int): TopDocs
    fun documentById(id: String): Int?
}
class EmptyIndex(override val tokenizer: GenericTokenizer = WhitespaceTokenizer()) : IIndex {
    override val defaultField: String = "missing"
    override val totalDocuments: Int = 0
    override fun fieldStats(field: String): CountStats? = null
    override fun getStats(expr: QExpr): CountStats = CountStats("EmptyIndex($expr)")
    override fun getStats(term: Term): CountStats = CountStats("EmptyIndex($term)")
    override fun close() { }
    override fun search(q: QExpr, n: Int): TopDocs = TopDocs(0L, emptyArray(), -Float.MAX_VALUE)
    override fun getRREnv(): RREnv = error("No RREnv for EmptyIndex.")
    override fun documentById(id: String): Int? = null
}

class IreneIndex(val io: RefCountedIO, params: IndexParams) : IIndex {
    constructor(params: IndexParams) : this(params.directory!!, params)
    val jobPool = ForkJoinPool.commonPool()
    val idFieldName = params.idFieldName
    val reader = DirectoryReader.open(io.open().use())
    val searcher = IndexSearcher(reader, jobPool)
    val analyzer = params.analyzer
    val env = IreneQueryLanguage(this).apply {
        defaultField = params.defaultField
    }
    override val tokenizer: LuceneTokenizer = LuceneTokenizer(analyzer)
    override val defaultField: String get() = env.defaultField
    override val totalDocuments: Int get() = reader.numDocs()
    override fun getRREnv(): RREnv = env

    private val termStatsCache: Cache<Term, CountStats> = Caffeine.newBuilder().maximumSize(100_000).build()
    private val exprStatsCache = Caffeine.newBuilder().maximumSize(100_000).build<QExpr, ForkJoinTask<CountStats>>()
    private val fieldStatsCache = ConcurrentHashMap<String, CountStats?>()
    private val nameToIdCache: Cache<String, Int> = Caffeine.newBuilder().maximumSize(100_000).build()

    override fun close() {
        reader.close()
        io.close()
    }

    fun getField(doc: Int, name: String): IndexableField? = searcher.doc(doc, setOf(name))?.getField(name)
    fun getDocumentName(doc: Int): String? {
        return getField(doc, idFieldName)?.stringValue()
    }
    fun docAsParameters(doc: Int): Parameters? {
        val ldoc = document(doc) ?: return null
        val fields = Parameters.create()
        ldoc.fields.forEach { field ->
            val name = field.name()!!
            fields.putIfNotNull(name, field.stringValue())
            fields.putIfNotNull(name, field.numericValue())
        }
        return fields
    }
    fun document(doc: Int): LDoc? {
        return lucene_try { searcher.doc(doc) }
    }
    fun document(doc: Int, fields: Set<String>): LDoc? {
        return lucene_try { searcher.doc(doc, fields) }
    }
    private fun documentByIdInternal(id: String): Int? {
        val q = BooleanQuery.Builder().add(TermQuery(Term(idFieldName, id)), BooleanClause.Occur.MUST).build()!!
        return lucene_try {
            val results = searcher.search(q, 10)?.scoreDocs
            if (results == null || results.isEmpty()) return null
            // TODO complain about dupes?
            return results[0].doc
        }
    }
    override fun documentById(id: String): Int? {
        val response = nameToIdCache.get(id, { missing -> documentByIdInternal(missing) ?: -1 })
        if (response == null || response < 0) return null
        return response
    }

    fun terms(doc: Int, field: String): List<String> {
        val text = getField(doc, field)?.stringValue() ?: return emptyList()
        return tokenize(text, field)
    }

    fun getAverageDL(field: String): Double = fieldStats(field)?.avgDL() ?: error("No such field $field.")

    override fun fieldStats(field: String): CountStats? {
        return fieldStatsCache.computeIfAbsent(field, {
            CalculateStatistics.fieldStats(searcher, field)
        })
    }

    override fun getStats(term: Term): CountStats {
        //println("getStats($term)")
        return termStatsCache.get(term, {CalculateStatistics.lookupTermStatistics(searcher, it)})
                ?: fieldStats(term.field())
                ?: error("No such field ${term.field()}.")
    }
    override fun getStats(expr: QExpr): CountStats {
        if (expr is TextExpr) {
            return expr.stats ?: getStats(expr.text, expr.statsField())
        }
        return getExprStats(expr)!!.join()
    }

    fun prepare(expr: QExpr): IreneQueryModel = IreneQueryModel(this, this.env, expr)

    private fun getExprStats(expr: QExpr): ForkJoinTask<CountStats>? {
        return exprStatsCache.get(expr, { missing ->
            val func: ()->CountStats = {CalculateStatistics.computeQueryStats(searcher, prepare(missing), this::fieldStats)}
            jobPool.submit(func)
        })
    }

    override fun search(q: QExpr, n: Int): TopDocs {
        return searcher.search(prepare(q), TopKCollectorManager(n))!!
    }

    fun pool(qs: Map<String, QExpr>, depth: Int): Map<String, TopDocs> {
        val multiExpr = MultiExpr(qs)
        return searcher.search(prepare(multiExpr), PoolingCollectorManager(multiExpr, depth))
    }

    fun explain(q: QExpr, doc: Int): Explanation = searcher.explain(prepare(q), doc)
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
        withPath(File("/mnt/scratch/jfoley/robust.irene2"))
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

