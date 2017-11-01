package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.sprf.GExpr
import edu.umass.cics.ciir.sprf.inqueryStop
import edu.umass.cics.ciir.sprf.setf
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.core.retrieval.query.Node
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator

/**
 * @author jfoley
 */

data class LTRDoc(val name: String, val features: Map<String, Double>, val rank: Int, val tokenized: String) {
    val terms: List<String>
        get() = tokenized.split(" ")
    val freqs = BagOfWords(terms)

    constructor(p: Parameters): this(p.getString("id"),
            hashMapOf(
                    Pair("title-ql", p.getDouble("title-ql")),
                    Pair("title-ql-prior", p.getDouble("title-ql-prior"))),
            p.getInt("rank"),
            p.getString("tokenized"))
}

data class LTRQuery(val qid: String, val qtext: String, val qterms: List<String>, val docs: List<LTRDoc>)

fun forEachQuery(dsName: String, doFn: (LTRQuery) -> Unit) {
    StreamCreator.openInputStream("$dsName.qlpool.jsonl.gz").reader().useLines { lines ->
        lines.map { Parameters.parseStringOrDie(it) }.forEach { qjson ->
            val qid = qjson.getString("qid")
            val qtext = qjson.getString("qtext")
            val qterms = qjson.getAsList("qterms", String::class.java)

            val docs = qjson.getAsList("docs", Parameters::class.java).map { LTRDoc(it) }

            doFn(LTRQuery(qid, qtext, qterms, docs))
        }
    }
}

data class WeightedTerm(val score: Double, val term: String) : Comparable<WeightedTerm> {
    // Natural order: biggest first.
    override fun compareTo(other: WeightedTerm): Int {
        val cmp = -java.lang.Double.compare(score, other.score)
        if (cmp != 0) return cmp
        return term.compareTo(other.term)
    }
}

data class RelevanceModel(val weights: TObjectDoubleHashMap<String>) {
    val total = weights.values().sum()
    fun toTerms(): List<WeightedTerm> {
        val output = ArrayList<WeightedTerm>(weights.size())
        weights.forEachEntry {term, weight ->
            output.add(WeightedTerm(weight, term))
        }
        return output
    }
    fun toTerms(k: Int): List<WeightedTerm> = toTerms().sorted().take(k)
    fun toQuery(k: Int): GExpr = GExpr("combine").apply {
        toTerms(k).forEachIndexed { i, wt ->
            setf("$i", wt.score)
            addChild(Node.Text(wt.term))
        }
    }
}

class BagOfWords(terms: List<String>) {
    val counts = TObjectIntHashMap<String>()
    val length = terms.size.toDouble()
    init {
        terms.forEach { counts.adjustOrPutValue(it, 1, 1) }
    }
    fun prob(term: String): Double = counts.get(term) / length
}

fun computeRelevanceModel(docs: List<LTRDoc>, feature: String, depth: Int, flat: Boolean = false, stopwords: Set<String> = inqueryStop): RelevanceModel {
    val fbdocs = docs.sortedByDescending { it.features[feature]!! }.take(depth)

    val rmModel = TObjectDoubleHashMap<String>()
    fbdocs.forEach { doc ->
        val local = doc.freqs.counts
        val length = doc.freqs.length

        val prior = if (flat) 1.0 else doc.features[feature]!!
        local.forEachEntry {term, count ->
            if (inqueryStop.contains(term)) return@forEachEntry true
            val prob = prior * count.toDouble() / length
            rmModel.adjustOrPutValue(term, prob, prob)
            true
        }
    }

    return RelevanceModel(rmModel)
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "robust")
    //val dataset = DataPaths.get(dsName)
    forEachQuery(dsName) {
        val rm = computeRelevanceModel(it.docs, "title-ql-prior", 10)
        val wt = rm.toTerms(100)
        //println(it.qterms)
        //println(exp.collectTerms())

        it.docs.forEach { doc ->
            val local = TObjectIntHashMap<String>()
            val terms = doc.terms
            val length = terms.size.toDouble()
            terms.forEach { local.adjustOrPutValue(it, 1, 1) }

        }
    }
}