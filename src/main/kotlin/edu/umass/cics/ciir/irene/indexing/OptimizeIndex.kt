package edu.umass.cics.ciir.irene.indexing

import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.sprf.DataPaths
import org.apache.lucene.util.InfoStream
import org.apache.lucene.util.PrintStreamInfoStream
import org.lemurproject.galago.utility.Parameters

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    InfoStream.setDefault(PrintStreamInfoStream(System.out))

    val targetSegments = argp.get("targetSegments", 8)
    val dataset = argp.get("dataset", "gov2")

    val ip = DataPaths.get(dataset).getIndexParams()
    ip.append()
    IreneIndexer(ip).use { writer ->
        writer.writer.forceMerge(targetSegments)
    }
}