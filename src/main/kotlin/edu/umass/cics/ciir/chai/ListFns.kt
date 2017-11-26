package edu.umass.cics.ciir.chai

import edu.umass.cics.ciir.sprf.incr
import java.util.*
import kotlin.streams.toList

/**
 * @author jfoley
 */

fun <T>  Iterable<T>.shuffled(rand: Random = Random()): ArrayList<T> {
    val x = this.toCollection(ArrayList<T>())
    Collections.shuffle(x, rand)
    return x
}
fun <T, L: MutableList<T>> L.shuffle(rand: Random = Random()): L {
    Collections.shuffle(this, rand)
    return this
}

fun <M: MutableMap<K, MutableList<V>>, K,V> M.push(k: K, v: V) {
    this.computeIfAbsent(k, { ArrayList<V>() }).add(v)
}

fun <N : Number> List<N>.mean(): Double {
    if (this.isEmpty()) return 0.0
    if (this.size == 1) return this[0].toDouble()
    return this.sumByDouble { it.toDouble() } / this.size.toDouble()
}

fun <N : Number> List<N>.asFeatureStats(): FeatureStats {
    val out = FeatureStats()
    for (x in this) {
        out.push(x.toDouble())
    }
    return out
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

fun <T> Map<T, Double>.normalize(): Map<T, Double> {
    val norm = this.values.sum()
    return this.mapValues { (_,v) -> v/norm }
}

fun <T> List<T>.pfor(doFn: (T)->Unit) {
    this.parallelStream().forEach(doFn)
}
fun <I,O> Collection<I>.pmap(mapper: (I)->O): List<O> = this.parallelStream().map(mapper).sequential().toList()

class FeatureStats {
    var min = Double.MAX_VALUE
    var max = -Double.MAX_VALUE
    var sum = 0.0
    var count = 0
    fun push(x: Double) {
        count++
        sum += x
        if (x < min) {
            min = x;
        }
        if (x > max) {
            max = x
        }
    }

    override fun toString(): String = "$count $min..$max"

    fun normalize(y: Double): Double {
        if (count == 0) return y
        if (min == max) return y
        return (y - min) / (max - min)
    }
}
