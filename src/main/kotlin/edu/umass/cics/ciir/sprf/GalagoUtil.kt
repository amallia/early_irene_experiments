package edu.umass.cics.ciir.sprf

import org.lemurproject.galago.core.eval.QueryResults

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

