package edu.umass.cics.ciir.chai

import edu.umass.cics.ciir.sprf.printer
import org.lemurproject.galago.utility.StreamCreator
import java.io.File
import java.io.PrintWriter

/**
 * @author jfoley
 */

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
fun File.smartReader() = StreamCreator.openInputStream(this).bufferedReader()
fun File.smartPrinter() = StreamCreator.openOutputStream(this).printer()
fun <T> File.smartLines(block: (Sequence<String>)->T): T = smartReader().useLines(block)
fun File.smartDoLines(doProgress: Boolean=false, handler: (String)->Unit) {
    val msg = Debouncer()
    var done = 0L
    smartReader().useLines { lines ->
        lines.forEach { line ->
            handler(line)
            done++
            if (doProgress && msg.ready()) {
                println(msg.estimate(done, done))
            }
        }
    }
    if (doProgress) {
        println(msg.estimate(done, done))
    }
}
fun File.smartPrint(block: (PrintWriter)->Unit) = StreamCreator.openOutputStream(this).printer().use(block)
