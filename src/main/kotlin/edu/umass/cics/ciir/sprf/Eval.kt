package edu.umass.cics.ciir.sprf

import gnu.trove.list.array.TDoubleArrayList
import org.lemurproject.galago.core.eval.metric.QueryEvaluatorFactory
import org.lemurproject.galago.utility.Parameters
import java.util.*

/**
 * @author jfoley
 */
class NamedMeasures {
    val measures = HashMap<String, TDoubleArrayList>()
    fun push(what: String, x: Double) {
        measures.computeIfAbsent(what, { TDoubleArrayList() }).add(x)
    }
    fun means(): TreeMap<String, Double> = measures.mapValuesTo(TreeMap()) { (_,arr) -> arr.mean() }
}

fun getEvaluators(metricNames: List<String>) = metricNames.associate { Pair(it, QueryEvaluatorFactory.create(it, Parameters.create())) }

val LN2 = Math.log(2.0)
fun log2(x: Double): Double = Math.log(x) / LN2

fun TDoubleArrayList.mean(): Double {
    if (this.size() == 0) return 0.0;
    return this.sum() / this.size().toDouble()
}

fun TDoubleArrayList.logProb(orElse: Double): Double {
    if (this.size() == 0) return orElse;
    return Math.log(this.sum() / this.size().toDouble())
}

fun<K> MutableMap<K, Double>.incr(key: K, amount: Double) {
    this.compute(key) { _,prev -> (prev ?: 0.0) + amount }
}
fun<K> MutableMap<K, Int>.incr(key: K, amount: Int) {
    this.compute(key) { _,prev -> (prev ?: 0) + amount }
}
