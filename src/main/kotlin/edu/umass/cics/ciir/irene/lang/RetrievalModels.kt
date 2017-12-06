package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.chai.forAllPairs
import edu.umass.cics.ciir.chai.forEachSeqPair

/**
 *
 * @author jfoley.
 */
fun SmartStop(terms: List<String>, stopwords: Set<String>): List<String> {
    val nonStop = terms.filter { !stopwords.contains(it) }
    if (nonStop.isEmpty()) {
        return terms
    }
    return nonStop
}

// Easy "model"-based constructor.
fun QueryLikelihood(terms: List<String>, field: String?=null, statsField: String?=null, mu: Double? = null): QExpr {
    return UnigramRetrievalModel(terms, { DirQLExpr(it, mu) }, field, statsField)
}
fun BM25Model(terms: List<String>, field: String?=null, statsField: String?=null, b: Double? = null, k: Double? = null): QExpr {
    return UnigramRetrievalModel(terms, { BM25Expr(it, b, k) }, field, statsField)
}
fun UnigramRetrievalModel(terms: List<String>, scorer: (TextExpr)-> QExpr, field: String?=null, statsField: String?=null): QExpr {
    return MeanExpr(terms.map { scorer(TextExpr(it, field, statsField)) })
}

fun SequentialDependenceModel(terms: List<String>, field: String?=null, statsField: String?=null, stopwords: Set<String> =emptySet(), uniW: Double = 0.8, odW: Double = 0.15, uwW: Double = 0.05, odStep: Int=1, uwWidth:Int=8, fullProx: Double? = null, fullProxWidth:Int=12, makeScorer: (QExpr)-> QExpr = { DirQLExpr(it) }): QExpr {
    if (terms.isEmpty()) throw IllegalStateException("Empty SDM")
    if (terms.size == 1) {
        return makeScorer(TextExpr(terms[0], field, statsField))
    }

    val nonStop = terms.filterNot { stopwords.contains(it) }
    val bestTerms = (if (nonStop.isNotEmpty()) { nonStop } else terms)
    val unigrams: List<QExpr> = bestTerms
            .map { makeScorer(TextExpr(it, field, statsField)) }

    val bigrams = ArrayList<QExpr>()
    val ubigrams = ArrayList<QExpr>()
    terms.forEachSeqPair { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        bigrams.add(makeScorer(OrderedWindowExpr(ts, odStep)))
    }
    bestTerms.forEachSeqPair { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        ubigrams.add(makeScorer(UnorderedWindowExpr(ts, uwWidth)))
    }

    val exprs = arrayListOf(
            MeanExpr(unigrams).weighted(uniW),
            MeanExpr(bigrams).weighted(odW),
            MeanExpr(ubigrams).weighted(uwW))

    if (fullProx != null) {
        val fullProxTerms = if (bestTerms.size >= 2) bestTerms else terms
        exprs.add(makeScorer(UnorderedWindowExpr(fullProxTerms.map { TextExpr(it, field, statsField) }, fullProxWidth)).weighted(fullProx))
    }

    return MeanExpr(exprs)
}

fun FullDependenceModel(terms: List<String>, field: String?=null, statsField: String? = null, stopwords: Set<String> =emptySet(), uniW: Double = 0.8, odW: Double = 0.15, uwW: Double = 0.05, odStep: Int=1, uwWidth:Int=8, fullProx: Double? = null, fullProxWidth:Int=12, makeScorer: (QExpr)-> QExpr = { DirQLExpr(it) }): QExpr {
    if (terms.isEmpty()) throw IllegalStateException("Empty FDM")
    if (terms.size == 1) {
        return makeScorer(TextExpr(terms[0], field, statsField))
    }

    val nonStop = terms.filterNot { stopwords.contains(it) }
    val bestTerms = (if (nonStop.isNotEmpty()) { nonStop } else terms)
    val unigrams: List<QExpr> = bestTerms
            .map { makeScorer(TextExpr(it, field, statsField)) }

    val bigrams = ArrayList<QExpr>()
    val ubigrams = ArrayList<QExpr>()
    terms.forAllPairs { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        bigrams.add(makeScorer(OrderedWindowExpr(ts, odStep)))
    }
    bestTerms.forAllPairs { lhs, rhs ->
        val ts = listOf(lhs, rhs).map { TextExpr(it, field, statsField) }
        ubigrams.add(makeScorer(UnorderedWindowExpr(ts, uwWidth)))
    }

    val exprs = arrayListOf(
            MeanExpr(unigrams).weighted(uniW),
            MeanExpr(bigrams).weighted(odW),
            MeanExpr(ubigrams).weighted(uwW))

    if (fullProx != null) {
        val fullProxTerms = if (bestTerms.size >= 2) bestTerms else terms
        exprs.add(makeScorer(UnorderedWindowExpr(fullProxTerms.map { TextExpr(it, field, statsField) }, fullProxWidth)).weighted(fullProx))
    }

    return SumExpr(exprs)
}

