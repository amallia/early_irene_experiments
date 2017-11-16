package edu.umass.cics.ciir.chai

import edu.umass.cics.ciir.sprf.printer
import org.lemurproject.galago.utility.StreamCreator
import java.io.Closeable
import java.io.File
import java.io.InputStream
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
inline fun <T> File.smartLines(block: (Sequence<String>)->T): T = smartReader().useLines(block)
fun File.smartDoLines(doProgress: Boolean=false, limit: Int? = null,  total: Long? = null, handler: (String)->Unit) {
    val msg = Debouncer()
    var done = 0L
    smartLines { lines ->
        val lineSeq = if (limit != null) {
            lines.take(limit)
        } else {
            lines
        }
        val count = limit?.toLong() ?: total
        lineSeq.forEach { line ->
            handler(line)
            done++
            if (doProgress && msg.ready()) {
                println(msg.estimate(done, count ?: done))
            }
        }
    }
    if (doProgress) {
        println(msg.estimate(done, done))
    }
}
fun File.smartPrint(block: (PrintWriter)->Unit) = StreamCreator.openOutputStream(this).printer().use(block)

fun openResource(path: String): InputStream {
    val target = if (path[0] != '/') { "/$path" } else { path }
    return String::class.java.getResourceAsStream(target)
}
fun resourceLines(path: String, block: (String)->Unit) = openResource(path).reader().useLines{lines -> lines.forEach(block) }

fun <A :Closeable, B: Closeable> Pair<A,B>.use(block: (A, B)->Unit) {
    this.first.use { a ->
        this.second.use { b ->
            block(a, b)
        }
    }
}

fun closeOrError(c: Closeable): Exception? = try {
    c.close()
    null;
} catch (e: Exception) {
    e;
}

fun closeAll(items: List<Closeable>) {
    var closed = true
    val errors = items.map { closeOrError(it) }
    if (errors.isEmpty()) {
        return
    }
    val err = RuntimeException()
    errors.forEach { err.addSuppressed(it) }
    throw err
}

inline fun <T> useAll(items: List<Closeable>, block: ()->T): T {
    var closed = false
    try {
        return block()
    } catch (e : Exception) {
        closed = true
        closeAll(items)
        throw e
    } finally {
        if (!closed) {
            closeAll(items)
        }
    }
}