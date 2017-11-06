package edu.umass.cics.ciir.sprf

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.net.InetAddress

fun notImpl(msg: String): Nothing = throw RuntimeException("notImpl: $msg")
fun isSydney(host: String): Boolean {
    return host == "sydney.cs.umass.edu" || host.startsWith("sydney") || host.endsWith("sydney")
}
fun mapHost(host: String): String {
    if (isSydney(host)) {
        return "sydney"
    }
    return host
}


interface IRDataset {
    companion object {
        val host: String = mapHost(InetAddress.getLocalHost().hostName)
    }
    fun getQueryDir(): File = File(when(host) {
        "gob" -> "/home/jfoley/code/queries/"
        "oakey" -> "/home/jfoley/code/queries/"
        "sydney" -> "/mnt/nfs/work1/jfoley/queries/"
        else -> notImpl(host)
    })

    fun getIndex(): LocalRetrieval = getIndex(Parameters.create())
    fun getIndex(p: Parameters): LocalRetrieval = LocalRetrieval(getIndexFile().absolutePath, p)

    fun parseTSV(fp: File): Map<String, String> = fp.useLines { lines ->
        lines.associate { line ->
            val cols = line.trim().split("\t")
            assert(cols.size == 2)
            Pair(cols[0], cols[1])
        }
    }

    val qrels: QuerySetJudgments get() = getQueryJudgments()
    val title_qs: Map<String,String> get() = getTitleQueries()
    val desc_qs: Map<String,String> get() = getDescQueries()

    fun getTitleQueries(): Map<String, String> = parseTSV(getTitleQueryFile())
    fun getDescQueries(): Map<String, String> = parseTSV(getDescQueryFile())
    fun getQueryJudgments(): QuerySetJudgments = QuerySetJudgments(getQueryJudgmentsFile().absolutePath, false, false);

    // Implement these:
    fun getIndexFile(): File
    fun getTitleQueryFile(): File
    fun getDescQueryFile(): File
    fun getQueryJudgmentsFile(): File
}

class Robust04 : IRDataset {
    override fun getIndexFile(): File = File(when(IRDataset.host) {
        "gob" -> "/media/jfoley/flash/robust04.galago"
        "oakey" -> "/mnt/scratch/jfoley/robust04.galago/"
        else -> notImpl(IRDataset.host)
    })
    override fun getTitleQueryFile(): File = File(getQueryDir(), "robust04/rob04.titles.tsv")
    override fun getDescQueryFile(): File = File(getQueryDir(), "robust04/rob04.descs.tsv")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "robust04/robust04.qrels")
}

class Gov2 : IRDataset {
    override fun getIndexFile(): File = File(when(IRDataset.host) {
        "oakey" -> "/mnt/scratch/jfoley/gov2.galago/"
        "sydney" -> "/mnt/nfs/work3/sjh/indexes/gov2.index/"
        else -> notImpl(IRDataset.host)
    })

    override fun getTitleQueryFile(): File = File(getQueryDir(), "gov2/gov2.titles.tsv")
    override fun getDescQueryFile(): File = File(getQueryDir(), "gov2/gov2.descs.tsv")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "gov2/gov2.qrels")
}

abstract class WikiSource : IRDataset {
    override fun getIndexFile(): File = notImpl(IRDataset.host)
    fun getIreneIndex() = IreneIndex(IndexParams().apply {
        withAnalyzer("categories", WhitespaceAnalyzer())
        withPath(File(when(IRDataset.host) {
            "oakey" -> "/mnt/scratch/jfoley/dbpedia-2016-10/dbpedia.irene2/"
            "gob" -> "/media/jfoley/flash/dbpedia-2016-10/sample/dbpedia.shard0.irene2"
            else -> notImpl(IRDataset.host)
        }))
    })
}

class Clue12Rewq : WikiSource() {
    override fun getTitleQueryFile(): File = File(getQueryDir(), "clue12/web1314.queries.tsv")
    override fun getDescQueryFile(): File = File(getQueryDir(), "clue12/web1314.descs.tsv")
    override fun getQueryJudgmentsFile(): File = File(getQueryDir(), "rewq/clue12.mturk.qrel")
}

private class DBPE : WikiSource() {
    override fun getQueryDir(): File = File("deps/dbpedia-entity/collection/v2/")
    override fun getTitleQueryFile(): File = File(getQueryDir(), "queries-v2.txt")
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

/**
 * @author jfoley
 */
object DataPaths {
    val Robust = Robust04()
    val Gov2 = Gov2()

    val REWQ_Clue12: WikiSource = Clue12Rewq()
    val DBPE: WikiSource = DBPE()

    fun get(name: String): IRDataset = when(name) {
        "robust" -> Robust
        "robust04" -> Robust
        "gov2" -> Gov2
        else -> notImpl(name)
    }
}
