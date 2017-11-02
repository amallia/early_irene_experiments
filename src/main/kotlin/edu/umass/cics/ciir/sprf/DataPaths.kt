package edu.umass.cics.ciir.sprf

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

/**
 * @author jfoley
 */
object DataPaths {
    val Robust = Robust04()
    val Gov2 = Gov2()

    fun get(name: String): IRDataset = when(name) {
        "robust" -> Robust
        "robust04" -> Robust
        "gov2" -> Gov2
        else -> notImpl(name)
    }
}
