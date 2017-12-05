package edu.umass.cics.ciir.wordvec

import edu.umass.cics.ciir.chai.CountingDebouncer
import edu.umass.cics.ciir.chai.smartReader
import edu.umass.cics.ciir.iltr.pagerank.SpacesRegex
import edu.umass.cics.ciir.learning.Vector
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.FloatBuffer
import java.util.concurrent.ConcurrentHashMap
import kotlin.streams.asStream

/**
 * @author jfoley
 */

inline fun <T> boundsCheck(i: Int, N: Int, fn: ()->T): T {
    if (i < 0 || i >= N) error("Out of bounds error: $i $N.")
    return fn()
}

class FloatBufferVector(override val dim: Int, val buffer: FloatBuffer, val offset: Int) : Vector {
    override fun get(i: Int): Double = boundsCheck(i, dim) { buffer[offset+i].toDouble() }
    override fun toString(): String = "FBV${offset}"
}

class FloatVectorBuffer(val dim: Int, val n: Int) {
    val request = dim * n * 4
    val buffer = ByteBuffer.allocateDirect(dim * n * 4).asFloatBuffer()

    operator fun get(index : Int): FloatBufferVector = boundsCheck(index, n) {
        FloatBufferVector(dim, buffer, dim * index)
    }

    internal fun put(index: Int, data: FloatArray) {
        val offset = index*dim
        for (i in (0 until dim)) {
            buffer.put(offset + i, data[i])
        }
    }
}

interface Word2VecDB {
    val bgVector: Vector
    val dim: Int
    val N: Int
    operator fun get(term: String): Vector?
}

class InMemoryWord2VecDB(val terms: Map<String, Int>, val buffer: FloatVectorBuffer) : Word2VecDB {
    override val bgVector = buffer[0].copy()
    override val dim: Int = buffer.dim
    override val N: Int = terms.size
    override operator fun get(term: String): Vector? {
        val index = terms[term] ?: return null
        return buffer[index]
    }
}

fun loadWord2VecTextFormat(input: File): Word2VecDB {
    return input.smartReader().use { reader ->
        val firstLine = reader.readLine() ?: error("Empty file=$input")

        val cols = firstLine.split(SpacesRegex)
        val numVectors = cols[0].toInt()
        val numDimensions = cols[1].toInt()

        // Allocation may fail here:
        try {
            val storage = FloatVectorBuffer(numDimensions, numVectors)
            val terms = ConcurrentHashMap<String, Int>(numVectors)

            val msg = CountingDebouncer(numVectors.toLong())
            reader.lineSequence().mapIndexed {i, line -> Pair(i, line)}.asStream().parallel().forEach { (lineNo,line) ->
                val local = FloatArray(numDimensions)
                val items = line.split(' ')
                val term = items[0]
                for (i in (0 until numDimensions)) {
                    local[i] = items[i+1].toFloat()
                }
                storage.put(lineNo, local)
                terms.put(term, lineNo)

                msg.incr()?.let { upd ->
                    println("loadWord2VecTextFormat $input $upd")
                }
            }

            return InMemoryWord2VecDB(terms, storage)
        } catch (oom: OutOfMemoryError) {
            throw IOException("Couldn't load word vectors in RAM: $input.", oom)
        }
    }
}

fun main(args: Array<String>) {
    val db = loadWord2VecTextFormat(File("data/paragraphs.norm.w2v.vec"))
    println("Loaded ${db.N} vectors of dim ${db.dim}")

    println(db["president"])
    println(db["minister"])
}