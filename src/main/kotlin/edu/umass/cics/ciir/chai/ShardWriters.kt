package edu.umass.cics.ciir.chai

import org.lemurproject.galago.utility.StreamCreator
import java.io.Closeable
import java.io.File
import java.io.IOException
import java.io.PrintWriter

// This file is basically poor-man's map-reduce.

class ShardWriters(val outDir: File, val shards: Int = 10, name: String) : Closeable {
    init {
        println("PWD: "+ File("").absolutePath)
        outDir.ensureParentDirectories()
    }
    val outFiles: List<PrintWriter> = (0 until shards).map {
        val shardDir = File(outDir, "shard$it")
        if (!shardDir.ensureParentDirectories()) error("Cannot create $shardDir")
        val output = File(shardDir, name)
        println("Opening $output")
        PrintWriter(StreamCreator.openOutputStream(output).bufferedWriter())
    }

    operator fun get(i: Int): PrintWriter = outFiles[i]
    fun <T> hashed(obj: T): PrintWriter =
            outFiles[Math.abs(obj?.hashCode() ?: 0) % shards]

    override fun close() {
        val errs = outFiles.map {
            try {
                it.close()
                null
            } catch (throwable: Throwable) {
                throwable
            }
        }.filterNotNull()

        if (errs.isNotEmpty())
            throw IOException(errs.joinToString(separator = "\n\n"))
    }

}