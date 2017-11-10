package edu.umass.cics.ciir

import java.util.*

/**
 * @author jfoley
 */

class ReservoirSampler<T>(val numSamples: Int, val rand: Random = Random()) : AbstractList<T>() {
    val backing = ArrayList<T>(numSamples)
    var totalOffered = 0
    inline fun lazyProcess(getIfSampled: ()->T) {
        if (backing.size < numSamples) {
            backing.add(getIfSampled())
            return
        }

        // find position in virtual array, and store iff part of our sub-sampled view:
        val position = rand.nextInt(totalOffered)
        if (position > numSamples) return
        backing.set(position, getIfSampled())
    }
    fun process(element: T) {
        lazyProcess { element }
    }
    override fun add(element: T): Boolean {
        process(element)
        return true
    }
    override fun get(index: Int): T = backing[index]
    override val size: Int
        get() = numSamples
}