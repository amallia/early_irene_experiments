package edu.umass.cics.ciir.irene

import org.apache.lucene.index.PostingsEnum
import java.io.IOException

/**
 *
 * @author jfoley.
 */
typealias IOSystem = org.apache.lucene.store.Directory
typealias MemoryIO = org.apache.lucene.store.RAMDirectory
typealias DiskIO = org.apache.lucene.store.FSDirectory
typealias LDoc = org.apache.lucene.document.Document

class RefCountedIO(private val io: IOSystem) : java.io.Closeable
{
    var opened = 1
    override fun close() {
        synchronized(io) {
            if (--opened == 0) {
                io.close()
            }
        }
    }
    fun use(): IOSystem {
        assert(opened > 0)
        return io
    }
    fun open(): RefCountedIO {
        synchronized(io) {
            opened++
        }
        return this
    }
}

inline fun <T> lucene_try(action: ()->T): T? {
    return try {
        action()
    } catch (missing: IllegalArgumentException) {
        null
    } catch (ioe: IOException) {
        null
    }
}

enum class DataNeeded(val level: Int) {
    DOCS(0), COUNTS(1), POSITIONS(2);
    fun flags(): Int = when(this) {
        DataNeeded.DOCS -> PostingsEnum.NONE.toInt()
        DataNeeded.COUNTS -> PostingsEnum.FREQS.toInt()
        DataNeeded.POSITIONS -> PostingsEnum.ALL.toInt()
    }
    companion object {
        fun max(lhs: DataNeeded?, rhs: DataNeeded?): DataNeeded {
            val max = maxOf(lhs?.level ?: 0, rhs?.level ?: 0)
            return when(max) {
                0 -> DOCS
                1 -> COUNTS
                2 -> POSITIONS
                else -> error("Invalid Level Needed: max($lhs,$rhs)")
            }
        }
    }
}
