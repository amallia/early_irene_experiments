package edu.umass.cics.ciir.sprf

import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.index.stats.FieldStatistics
import org.lemurproject.galago.core.index.stats.NodeStatistics
import org.lemurproject.galago.core.retrieval.iterator.CountIterator
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator
import org.lemurproject.galago.core.retrieval.iterator.TransformIterator
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode
import org.lemurproject.galago.core.retrieval.query.NodeParameters
import org.lemurproject.galago.core.util.WordLists
import org.lemurproject.galago.utility.Parameters

/**
 * @author jfoley
 */
typealias GExpr = org.lemurproject.galago.core.retrieval.query.Node
typealias GResults = org.lemurproject.galago.core.retrieval.Results

val inqueryStop: Set<String> = WordLists.getWordListOrDie("inquery")

fun GExpr.push(what: GExpr): GExpr {
    this.addChild(what)
    return this
}
fun GExpr.setf(key: String, v: Int): GExpr = this.setf(key, v.toLong())
fun GExpr.setf(key: String, v: Long): GExpr {
    this.nodeParameters!!.set(key, v)
    return this
}
fun GExpr.setf(key: String, v: Boolean): GExpr {
    this.nodeParameters!!.set(key, v)
    return this
}
fun GExpr.setf(key: String, v: Double): GExpr {
    this.nodeParameters!!.set(key, v)
    return this
}
fun GExpr.setf(key: String, v: String): GExpr {
    this.nodeParameters!!.set(key, v)
    return this
}

fun GExpr.collectTerms(): String {
    val sb = ArrayList<String>()
    collectTerms(this, sb)
    return sb.joinToString(separator = " ")
}
fun collectTerms(q: GExpr, out: MutableList<String>) {
    if (q.isText) {
        out.add(q.defaultParameter)
    } else {
        q.childIterator.forEach { collectTerms(it, out) }
    }
}

fun GResults.toQueryResults(): QueryResults = QueryResults(this.scoredDocuments)

fun NodeStatistics.cfProbability(fieldStats: FieldStatistics): Double = this.nodeFrequency.toDouble() / fieldStats.collectionLength.toDouble()

class CountToScoreIter(val np: NodeParameters, val iter: CountIterator) : TransformIterator(iter), ScoreIterator {
    override fun getAnnotatedNode(sc: ScoringContext?): AnnotatedNode {
        return iter.getAnnotatedNode(sc)
    }

    override fun score(c: ScoringContext?): Double {
        return iter.count(c).toDouble()
    }

    override fun maximumScore(): Double {
        return Int.MAX_VALUE.toDouble()
    }

    override fun minimumScore(): Double {
        return 0.0
    }

}


val extraOpIndexParams = Parameters.create().apply {
    put("operators", Parameters.create().apply {
        set("count-to-score", CountToScoreIter::class.java.canonicalName)
    })
}

fun pmake(block: Parameters.() -> Unit): Parameters = Parameters.create().apply(block)

