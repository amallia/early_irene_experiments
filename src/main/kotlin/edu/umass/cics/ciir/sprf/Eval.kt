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

