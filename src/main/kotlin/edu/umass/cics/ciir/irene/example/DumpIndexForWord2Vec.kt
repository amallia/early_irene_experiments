package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.CountingDebouncer
import edu.umass.cics.ciir.chai.smartPrint
import edu.umass.cics.ciir.iltr.pagerank.SpacesRegex
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.getStr
import org.lemurproject.galago.utility.Parameters
import java.io.File

/**
 *
 * @author jfoley.
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val field = argp.get("field", "text")

    val output = argp.get("output", "w2v.input")

    File(output).smartPrint { writer ->
        if (argp.isString("dataset")) {
            DataPaths.get(argp.getStr("dataset")).getIreneIndex()
        } else {
            IreneIndex(IndexParams().apply {
                withPath(File(argp.getStr("index")))
            })
        }.use { index ->
            val msg = CountingDebouncer(index.totalDocuments.toLong())

            (0 until index.totalDocuments).toList().parallelStream().map { num ->
                val result = index.getField(num, field)?.stringValue() ?: ""
                msg.incr()?.let { upd ->
                    println(upd)
                }
                result.replace(SpacesRegex, " ")
            }.sequential().forEach { text ->
                if (text.isNotBlank()) {
                    writer.print(text)
                    writer.print("\n\n")
                }
            }
        }
    }
}

