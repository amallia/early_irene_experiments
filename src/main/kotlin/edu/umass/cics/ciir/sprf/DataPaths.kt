package edu.umass.cics.ciir.sprf

import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.net.InetAddress

/**
 * @author jfoley
 */
object DataPaths {
    val host: String = InetAddress.getLocalHost().hostName
    fun notImpl(): Nothing = throw RuntimeException("notImpl")
    fun getRobustIndex(): LocalRetrieval = getRobustIndex(Parameters.create())
    fun getRobustIndex(p: Parameters): LocalRetrieval = LocalRetrieval(when(host) {
        "gob" -> "/media/jfoley/flash/robust04.galago"
        "oakey" -> "/mnt/scratch/jfoley/robust04.galago/"
        else -> notImpl()
    }, p)
    fun getQueryDir(): File = File(when(host) {
        "gob" -> "/home/jfoley/code/queries/robust04/"
        "oakey" -> "/home/jfoley/code/queries/robust04/"
        else -> notImpl()
    })
    fun getTitleQueryFile(): File = File(getQueryDir(), "rob04.titles.tsv")
    fun getDescQueryFile(): File = File(getQueryDir(), "rob04.descs.tsv")
    fun getQueryJudgmentsFile(): File = File(getQueryDir(), "robust04.qrels")

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
}
