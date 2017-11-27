package edu.umass.cics.ciir.learning

import java.util.*

/**
 *
 * @author jfoley.
 */
interface Vector {
    val dim: Int
    val indexes: IntRange get() = (0 until dim)
    val l1Norm: Double get() = (0 until dim).sumByDouble { Math.abs(this[it]) }
    val l2Norm: Double get() = (0 until dim).sumByDouble {
        val xi = this[it]
        return xi*xi
    }

    fun toList() = indexes.map { get(it) }
    operator fun get(i: Int): Double
    fun dotp(v: Vector): Double {
        assert(v.dim == dim)
        return (0 until dim).sumByDouble { i -> this[i] * v[i] }
    }
    fun copy(): SimpleDenseVector {
        val out = SimpleDenseVector(dim)
        (0 until dim).forEach {
            out[it] = this[it]
        }
        return out
    }
}
data class BiasedVector(val inner: Vector) {
    val dim: Int = inner.dim + 1
    operator fun get(i: Int): Double {
        if (i == inner.dim) return 1.0
        return inner[i]
    }
}
interface MutableVector : Vector {
    operator fun set(i: Int, y: Double)
    fun clearToRandom(rand: Random = Random()) {
        (0 until dim).forEach { i ->
            this[i] = rand.nextGaussian()
        }
    }
    fun scale(x: Double) {
        (0 until dim).forEach { this[it] *= x }
    }
    fun normalizeL1() { normalize(l1Norm) }
    fun normalizeL2() { normalize(l2Norm) }
    fun normalize(norm: Double) {
        (0 until dim).forEach { this[it] /= norm }
    }
    operator fun plusAssign(v : Vector) {
        assert(v.dim == dim)
        (0 until dim).forEach { i ->
            this[i] += v[i]
        }
    }
    operator fun minusAssign(v : Vector) {
        assert(v.dim == dim)
        (0 until dim).forEach { i ->
            this[i] -= v[i]
        }
    }

    // perceptron.
    fun incr(scalar: Double, v: Vector) {
        (0 until dim).forEach { i ->
            this[i] += scalar*v[i]
        }
    }

    fun copyFrom(v: Vector) {
        (0 until dim).forEach { i ->
            this[i] = v[i]
        }
    }
}

class SimpleDenseVector(override val dim: Int) : MutableVector {
    val data = DoubleArray(dim)
    override fun get(i: Int): Double = data[i]
    override fun set(i: Int, y: Double) { data[i] = y }
    override fun toString(): String = data.joinToString { "%1.3f".format(it) }
}

interface MachineLearningInput {
    // num instances
    val numInstances: Int
    // num features
    val numFeatures: Int
    // may shuffle between rounds
    fun shuffle()
    // get X[i]
    operator fun get(i: Int): Vector
    // get y[i]
    fun truth(i: Int): Boolean
    // get y[i] as needed
    fun label(i: Int): Int = if(truth(i)) { 1 } else { -1 }
}
