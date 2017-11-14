package edu.umass.cics.ciir.chai

import edu.umass.cics.ciir.sprf.incr
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

inline fun <T> List<T>.meanByDouble(mapper: (T)->Double): Double {
    if (this.isEmpty()) return 0.0
    if (this.size == 1) return mapper(this[0])
    return this.sumByDouble { mapper(it) } / this.size.toDouble()
}

inline fun <T> List<T>.forEachSeqPair(fn: (T,T)->Unit) {
    (0 until this.size-1).forEach { i ->
        fn(this[i], this[i+1])
    }
}
inline fun <T> List<T>.forAllPairs(fn: (T,T)->Unit) {
    (0 until this.size-1).forEach { i ->
        (i until this.size).forEach { j ->
            fn(this[i], this[j])
        }
    }
}
fun <T> List<T>.computeEntropy(): Double {
    val counts = HashMap<T, Int>(this.size/2)
    val length = this.size.toDouble()
    forEach { w -> counts.incr(w, 1) }
    val sum = counts.values.sumByDouble { tf ->
        val p = tf / length
        p * Math.log(p)
    }
    return -sum
}

