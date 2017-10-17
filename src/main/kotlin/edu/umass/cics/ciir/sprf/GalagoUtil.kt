package edu.umass.cics.ciir.sprf

import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.index.stats.FieldStatistics
import org.lemurproject.galago.core.index.stats.NodeStatistics

/**
 * @author jfoley
 */
typealias GExpr = org.lemurproject.galago.core.retrieval.query.Node
typealias GResults = org.lemurproject.galago.core.retrieval.Results

fun GExpr.push(what: GExpr): GExpr {
    this.addChild(what)
    return this
}
fun GResults.toQueryResults(): QueryResults = QueryResults(this.scoredDocuments)

fun NodeStatistics.cfProbability(fieldStats: FieldStatistics): Double = this.nodeFrequency.toDouble() / fieldStats.collectionLength.toDouble()

