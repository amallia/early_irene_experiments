package edu.umass.cics.ciir

import java.util.*

/**
 * @author jfoley
 */

fun <T>  Iterable<T>.shuffled(rand: Random = Random()) {
    val x = this.toCollection(ArrayList<T>())
    Collections.shuffle(x, rand)
}

fun <M: MutableMap<K, MutableList<V>>, K,V> M.push(k: K, v: V) {
    this.computeIfAbsent(k, { ArrayList<V>() }).add(v)
}

fun <N : Number> List<N>.mean(): Double {
    if (this.isEmpty()) return 0.0
    if (this.size == 1) return this[0].toDouble()
    return this.sumByDouble { it.toDouble() } / this.size.toDouble()
}