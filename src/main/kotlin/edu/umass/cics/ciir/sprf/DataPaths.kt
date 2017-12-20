package edu.umass.cics.ciir.sprf

import edu.umass.cics.ciir.chai.push
import edu.umass.cics.ciir.chai.try_or_empty
import edu.umass.cics.ciir.dbpedia.wikiTitleToText
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.example.NYT
import edu.umass.cics.ciir.irene.example.TrecCarDataset
import edu.umass.cics.ciir.irene.example.getTrecCarIndexParams
import edu.umass.cics.ciir.irene.example.loadTrecCarDataset
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.jsoup.Jsoup
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.net.InetAddress

fun notImpl(msg: String): Nothing = throw RuntimeException("notImpl: $msg")
fun isSydney(host: String): Boolean {
    return host == "sydney.cs.umass.edu" || host.startsWith("sydney") || host.endsWith("sydney") || host.startsWith("compute-")
}
fun mapHost(host: String): String {
    if (isSydney(host)) {
        return "sydney"
    }
    return host
}


abstract class IRDataset {
    companion object {
        val host: String = mapHost(InetAddress.getLocalHost().hostName)
    }
    open fun getQueryDir(): File = File(when(host) {
        "gob" -> "/home/jfoley/code/queries/"
        "oakey" -> "/home/jfoley/code/queries/"
        "sydney" -> "/mnt/nfs/work1/jfoley/queries/"
        else -> notImpl(host)
    })
    abstract val name: String

    abstract val textFields: Set<String>
    abstract val statsField: String

    open fun getBM25B(): Double? = null
    open fun getBM25K(): Double? = null

    abstract fun getIndexParams(): IndexParams
    open fun getIreneIndex(): IreneIndex = IreneIndex(getIndexParams())

    fun getIndex(): LocalRetrieval = getIndex(Parameters.create())
    fun getIndex(p: Parameters): LocalRetrieval = LocalRetrieval(getIndexFile().absolutePath, p)

    fun parseTSV(fp: File): Map<String, String> = fp.useLines { lines ->
        lines.associate { line ->
            val cols = line.trim().split("\t")
            assert(cols.size == 2)
            Pair(cols[0], cols[1])
        }
    }

    val qrels: QuerySetJudgments by lazy { getQueryJudgments() }
    val title_qs: Map<String,String> by lazy { getTitleQueries() }
    val desc_qs: Map<String,String> by lazy { getDescQueries() }

    fun createFolds(k: Int): List<TVTFold> {
        if (k <= 2) error("Need 3 or more folds (not k=$k) for Train/Validate/Test cross-validation.")
        val qids = (try_or_empty { qrels.keys } + try_or_empty { title_qs.keys } + try_or_empty {  desc_qs.keys }).sorted()
        if (qids.isEmpty()) {
            error("Must have qrels or queries in order to create folds...")
        }

        val grouped = HashMap<Int, MutableList<String>>()
        qids.forEachIndexed { i, qid -> grouped.push(i % k, qid) }

        return (0 until k).map { split ->
            val test = split
            val vali = (split + 1) % k
            val training = (0 until k).filterNot { it == test || it == vali }

            TVTFold(split,
                    training.flatMapTo(HashSet()) { grouped[it]!! },
                    grouped[vali]!!.toSet(),
                    grouped[test]!!.toSet())
        }
    }

    open fun getTitleQueries(): Map<String, String> = parseTSV(getTitleQueryFile())
    open fun getDescQueries(): Map<String, String> = parseTSV(getDescQueryFile())
    open fun getQueryJudgments(): QuerySetJudgments = QuerySetJudgments(getQueryJudgmentsFile().absolutePath, false, false);

    // Implement these:
    abstract fun getIndexFile(): File
    abstract fun getTitleQueryFile(): File
    abstract fun getDescQueryFile(): File
    abstract fun getQueryJudgmentsFile(): File
}

data class TVTFold(val ord: Int, val train: Set<String>, val vali: Set<String>, val test: Set<String>) {
    override fun hashCode(): Int = ord.hashCode()
    override fun equals(other: Any?): Boolean {
        if (other is TVTFold) {
            return ord == other.ord && test == other.test && vali == other.vali && train == other.train
        }
        return false
    }
}

class Robust04 : IRDataset() {
    override fun getIndexParams(): IndexParams {
        val path = when(IRDataset.host) {
            "gob" -> "/media/jfoley/flash/robust.irene2"
            "oakey" -> "/mnt/scratch/jfoley/robust.irene2"
            "sydney" -> "/mnt/nfs/work1/jfoley/indexes/robust.irene2"
            else -> notImpl(IRDataset.host)
        }
        return IndexParams().apply {
            withPath(File(path))
            defaultField = statsField
        }
    }

    override val statsField: String = "body"
    override val textFields: Set<String> = setOf("body", "title")

    override val name: String get() = "robust"
    override fun getIndexFile(): File = File(when(IRDataset.host) {
        "gob" -> "/media/jfoley/flash/robust04.galago"
        "oakey" -> "/mnt/scratch/jfoley/robust04.galago/"
        "sydney" -> "/mnt/nfs/work3/sjh/indexes/robust04.index"
        else -> notImpl(IRDataset.host)
    })
    override fun getTitleQueryFile(): File = File(getQueryDir(), "robust04/rob04.titles.tsv")
    override fun getDescQueryFile(): File = File(getQueryDir(), "robust04/rob04.descs.tsv")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "robust04/robust04.qrels")

    // granularity chosen at which all training splits agree.
    override fun getBM25B(): Double? = 0.25
    override fun getBM25K(): Double? = 0.75
}

open class Gov2 : IRDataset() {
    override fun getIndexParams(): IndexParams {
        val path = when(IRDataset.host) {
            "gob" -> "/media/jfoley/flash/gov2.irene2"
            "oakey" -> "/mnt/scratch/jfoley/gov2.irene2"
            "sydney" -> "/mnt/nfs/work1/jfoley/indexes/gov2.irene2"
            else -> notImpl(IRDataset.host)
        }
        return IndexParams().apply {
            withPath(File(path))
            defaultField = "document"
            withAnalyzer("url", WhitespaceAnalyzer())
        }
    }

    override val statsField: String = "document"
    override val textFields: Set<String> = setOf("title", "body", "document")
    override val name: String get() = "gov2"
    override fun getIndexFile(): File = File(when(IRDataset.host) {
        "oakey" -> "/mnt/scratch/jfoley/gov2.galago/"
        "sydney" -> "/mnt/nfs/work3/sjh/indexes/gov2.index/"
        else -> notImpl(IRDataset.host)
    })

    override fun getTitleQueryFile(): File = File(getQueryDir(), "gov2/gov2.titles.tsv")
    override fun getDescQueryFile(): File = File(getQueryDir(), "gov2/gov2.descs.tsv")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "gov2/gov2.qrels")
}

class MQ2007 : Gov2() {
    override val name: String get() = "mq2007"
    override fun getDescQueryFile(): File = error("No description queries in MQ2007")
    override fun getTitleQueryFile(): File = File(getQueryDir(), "million_query_track/gov2/mq.gov2.judged.tsv")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "million_query_track/gov2/mq.gov2.judged.qrel")
}

open class NYTSource : IRDataset() {
    override val textFields: Set<String> = setOf("title", "headline", "lead", "byline", "text", "classifier")
    override val statsField: String = "text"
    override fun getTitleQueryFile(): File = error("No queries.")
    override fun getDescQueryFile(): File = error("No queries.")
    override fun getQueryJudgmentsFile(): File = error("No queries.")
    override fun getIndexParams(): IndexParams = NYT.getIndexParams(getIreneIndexPath())
    fun getIreneIndexPath() = when(IRDataset.host) {
        "oakey" -> "/mnt/scratch/jfoley/nyt.irene2"
        "gob" -> "/media/jfoley/flash/nyt.irene2"
        else -> notImpl(IRDataset.host)
    }
    override fun getIndexFile(): File = notImpl(IRDataset.host)
    override val name: String get() = "nyt-ldc"
    val inputTarFiles: List<File> get() = when(IRDataset.host) {
        "oakey" -> NYT.Years.flatMap { year -> File("/mnt/scratch/jfoley/nyt-raw/data", year.toString()).listFiles().toList() }
        "gob" -> NYT.Years.map { year -> File("/media/jfoley/flash/raw/nyt/nyt-$year.tar") }
        else -> notImpl(IRDataset.host)
    }
}

data class ParsedTrecCoreQuery(val qid: String, val title: String, val desc: String, val narr: String)
class TrecCoreNIST : NYTSource() {
    val nistQueryData: List<ParsedTrecCoreQuery> by lazy { parseNISTSGMLQueries(File(getQueryDir(), "trec_core/core_nist.txt")) }
    val crowdQueryData: List<ParsedTrecCoreQuery> by lazy { parseNISTSGMLQueries(File(getQueryDir(), "trec_core/core_crowd.txt")) }
    val queryData: List<ParsedTrecCoreQuery> by lazy { nistQueryData + crowdQueryData }
    override val name: String get() = "trec-core"

    fun parseNISTSGMLQueries(fp: File): List<ParsedTrecCoreQuery> {
        val doc = Jsoup.parse(fp.readText(), "http://trec.nist.gov")
        return doc.select("top").map { query ->
            // Without close tags, these can get parsed as:
            // <title> Title <desc> Longer Text <narr> Narrative here </narr> </desc> </title>
            val qid = query.selectFirst("num").ownText().substringAfter("Number:")
            val title = query.selectFirst("title").ownText() ?: ""
            val desc = query.selectFirst("desc").ownText() ?: ""
            val narr = query.selectFirst("narr").ownText() ?: ""
            ParsedTrecCoreQuery(qid.trim(), title.trim(), desc.trim(), narr.trim())
        }
    }

    override fun getTitleQueries(): Map<String, String> {
        return nistQueryData.associate { Pair(it.qid, it.title) }
    }

    override fun getDescQueries(): Map<String, String> {
        return nistQueryData.associate { Pair(it.qid, it.desc) }
    }
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "trec_core/nist.qrels.txt")
}

data class NYTWikiCiteQuery(val qid: String, val pages: List<String>, val relevant: String) {
    val queries: List<String> by lazy { pages.map { wikiTitleToText(it) }}
}
class NYTWikiCite : NYTSource() {
    override val name: String get() = "nyt-cite"
    val wcqs: List<NYTWikiCiteQuery> by lazy {
        File(getQueryDir(), "trec_core/nyt.wikicites.jsonl").readLines().mapIndexed { i,line ->
            val p = Parameters.parseStringOrDie(line)
            NYTWikiCiteQuery(
                    qid="%03d".format(i),
                    pages=p.getAsList("titles", String::class.java),
                    // Hack judgment document ids to match index and NIST (no more leading zeros):
                    relevant=p.getStr("judgment").toInt().toString())
        }
    }

    override fun getTitleQueries(): Map<String, String> {
        return wcqs.associate { Pair(it.qid, it.queries[0]) }
    }

    override fun getQueryJudgments(): QuerySetJudgments {
        val qrels = wcqs.associate {
            Pair(it.qid, QueryJudgments(it.qid, mapOf(it.relevant to 1)))
        }
        return QuerySetJudgments(qrels)
    }
}

open class WikiSource : IRDataset() {
    override val name: String = "wiki"
    override val textFields: Set<String> = setOf("body", "short_text")
    override val statsField: String = "body"
    override fun getTitleQueryFile(): File = error("No queries.")
    override fun getDescQueryFile(): File = error("No queries.")
    override fun getQueryJudgmentsFile(): File = error("No queries.")
    override fun getIndexFile(): File = notImpl(IRDataset.host)
    override fun getIndexParams(): IndexParams {
        val path = when(IRDataset.host) {
            "gob" -> "/media/jfoley/flash/dbpedia-2016-10/dbpedia.shard0.irene2"
            "oakey" -> "/mnt/scratch/jfoley/dbpedia-2016-10/dbpedia.irene2"
            else -> notImpl(IRDataset.host)
        }
        return IndexParams().apply {
            withPath(File(path))
            withAnalyzer("categories", WhitespaceAnalyzer())
            defaultField = "short_text"
        }
    }
}

class Clue12Rewq : WikiSource() {
    override val name: String get() = "rewq-clue12"
    override fun getTitleQueryFile(): File = File(getQueryDir(), "clue12/web1314.queries.tsv")
    override fun getDescQueryFile(): File = File(getQueryDir(), "clue12/web1314.descs.tsv")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "rewq/clue12.mturk.qrel")
}

private class DBPE : WikiSource() {
    override val name: String get() = "dbpedia-entity-v2"
    override fun getQueryDir(): File = File("deps/dbpedia-entity/collection/v2/")
    override fun getTitleQueryFile(): File = File(getQueryDir(), "queries-v2_stopped.txt")
    override fun getDescQueryFile(): File = error("No description queries for this dataset.")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "qrels-v2.txt")

    val prefix = "<dbpedia:"
    val suffix = ">"
    fun extractPageId(x: String):String {
        if (x.startsWith(prefix) && x.endsWith(suffix)) {
            return x.substring(prefix.length, x.length - suffix.length)
        }
        error("Not well formatted!")
    }

    override fun getQueryJudgments(): QuerySetJudgments {
        val judgments = HashMap<String, HashMap<String, Int>>()
        getQueryJudgmentsFile().bufferedReader().forEachLine { line ->
            val cols = line.split("\t")
            if (cols.size == 4) {
                val qid = cols[0]
                assert(cols[1] == "Q0")
                val page = extractPageId(cols[2])
                val label = cols[3].toInt()

                judgments
                        .computeIfAbsent(qid, { HashMap<String, Int>() })
                        .put(page, label)
            } else if (cols.size == 0) {
                // Blank line.
            } else {
                error("Mal-formatted line: ${cols.toList()}")
            }
        }

        return QuerySetJudgments(judgments.mapValues { (qid, raw) ->  QueryJudgments(qid, raw) })
    }
}

class Clue09BSpam60 : IRDataset() {
    override fun getIndexParams(): IndexParams {
        val path = when(IRDataset.host) {
            "oakey" -> "/mnt/scratch/jfoley/clue09.irene2"
            "sydney" -> "/mnt/nfs/work1/jfoley/indexes/clue09.irene2"
            else -> notImpl(IRDataset.host)
        }
        return IndexParams().apply {
            withPath(File(path))
            defaultField = "document"
            withAnalyzer("url", WhitespaceAnalyzer())
        }
    }
    override val statsField: String = "document"
    override val textFields: Set<String> = setOf("title", "body", "document")
    override val name: String get() = "clue09bspam60"
    override fun getIndexFile(): File = File(when(IRDataset.host) {
        "sydney" -> "/mnt/nfs/work3/sjh/indexes/clueweb-09-b-spam60.index/"
        else -> notImpl(IRDataset.host)
    })
    override fun getTitleQueryFile(): File = File(getQueryDir(), "clue09/clueweb.titles.tsv")
    override fun getDescQueryFile(): File = File(getQueryDir(), "clue09/clueweb.descs.tsv")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "clue09/clueweb.qrels")
}

class WT10G : IRDataset() {
    override fun getIndexParams(): IndexParams {
        val path = when(IRDataset.host) {
            "oakey" -> "/mnt/scratch/jfoley/wt10g.irene2"
            "sydney" -> "/mnt/nfs/work1/jfoley/indexes/wt10g.irene2"
            else -> notImpl(IRDataset.host)
        }
        return IndexParams().apply {
            withPath(File(path))
            defaultField = "document"
            withAnalyzer("url", WhitespaceAnalyzer())
        }
    }
    override val statsField: String = "document"
    override val textFields: Set<String> = setOf("title", "body", "document")
    override val name: String get() = "wt10g"
    override fun getIndexFile(): File = File(when(IRDataset.host) {
        "sydney" -> "/mnt/nfs/work3/sjh/indexes/wt10g.index/"
        else -> notImpl(IRDataset.host)
    })
    override fun getTitleQueryFile(): File = File(getQueryDir(), "wt10g/wt10g.titles.tsv")
    override fun getDescQueryFile(): File = File(getQueryDir(), "wt10g/wt10g.descs.tsv")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "wt10g/wt10g.qrels")
}

class TrecCarTest200 : IRDataset() {
    override val textFields: Set<String> = setOf("text", "links")
    override val statsField: String = "text"
    override val name: String = "trec-car"
    fun getBaseDir(): File = File(when(host) {
        "oakey" -> "/mnt/scratch/jfoley/trec-car/"
        else -> notImpl(host)
    })
    val tcd: TrecCarDataset by lazy { loadTrecCarDataset(File(getBaseDir(), "test200/train.test200.fold0.cbor.hierarchical.qrels")) }

    override fun getIreneIndex(): IreneIndex = super.getIreneIndex().apply {
        env.indexedBigrams = true
    }
    override fun getIndexParams(): IndexParams = getTrecCarIndexParams(File(getBaseDir(), "pcorpus.irene2"))
    override fun getIndexFile(): File = error("No galago.")
    override fun getTitleQueryFile(): File = error("No title queries.")
    override fun getDescQueryFile(): File = error("No desc queries.")
    override fun getQueryJudgmentsFile(): File = error("No judgments file.")
    override fun getTitleQueries(): Map<String, String> = tcd.queries
    override fun getDescQueries(): Map<String, String> = tcd.queries
    override fun getQueryJudgments(): QuerySetJudgments = tcd.judgments
}

/**
 * @author jfoley
 */
object DataPaths {
    val Robust = Robust04()
    val Gov2 = Gov2()
    val Gov2_MQT = MQ2007()
    val Clue09BSpam60 = Clue09BSpam60()
    val WT10G = WT10G()
    val TrecCarT200 = TrecCarTest200()
    val TrecCoreNIST = TrecCoreNIST()
    val NYTWikiCite = NYTWikiCite()

    val REWQ_Clue12: WikiSource = Clue12Rewq()
    val DBPE: WikiSource = DBPE()

    fun get(name: String): IRDataset = when(name) {
        "wiki" -> WikiSource()
        "nyt-cite" -> NYTWikiCite
        "trec-core" -> TrecCoreNIST
        "trec-car-test200", "trec-car" -> TrecCarT200
        "robust", "robust04" -> Robust
        "gov2" -> Gov2
        "mq07", "gov2mqt" -> Gov2_MQT
        "clue09", "clue09b", "clue09bspam60" -> Clue09BSpam60
        "wt10g" -> WT10G
        else -> notImpl(name)
    }

    val QuerySets = listOf(Robust, Gov2, Gov2_MQT, Clue09BSpam60, WT10G, REWQ_Clue12, DBPE)

    @JvmStatic fun main(args: Array<String>) {
        QuerySets.forEach { collection ->
            val nj = collection.qrels.size
            val nq = collection.title_qs.size
            println("${collection.name} Queries: submitted: $nq == with-qrels: $nj")
        }

        val qids = (Gov2.title_qs.keys + Gov2.getQueryJudgments().keys).toSortedSet().joinToString(separator = " ")

        println(qids)
    }
}
