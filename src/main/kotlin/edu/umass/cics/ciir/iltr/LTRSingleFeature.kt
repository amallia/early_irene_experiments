package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.sprf.*
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.core.eval.EvalDoc
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.retrieval.query.Node
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import org.lemurproject.galago.utility.lists.Ranked

/**
 * @author jfoley
 */

class LTRDocByFeature(val feature: String, val doc: LTRDoc, rank: Int, score: Double) : EvalDoc, Ranked(rank, score) {
    override fun getRank(): Int = rank
    override fun getScore(): Double = score
    override fun getName(): String = doc.name
    override fun clone(score: Double): LTRDocByFeature = LTRDocByFeature(feature, doc, rank, score)
    constructor(feature: String, doc: LTRDoc) : this(feature, doc, -1, doc.features[feature]!!)
}

data class LTRDoc(val name: String, val features: HashMap<String, Double>, val rank: Int, val tokenized: String) {
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

data class LTRQuery(val qid: String, val qtext: String, val qterms: List<String>, val docs: List<LTRDoc>) {
    fun ranked(ftr: String): ArrayList<LTRDocByFeature> = docs.mapTo(ArrayList<LTRDocByFeature>(docs.size)) { LTRDocByFeature(ftr, it) }
    fun toQResults(ftr: String): QueryResults {
        val ranked = ranked(ftr)
        Ranked.setRanksByScore(ranked)
        return QueryResults(ranked)
    }

    fun whitelistParameters(): Parameters {
        val whitelist = docs.map { it.name }.toList()
        return Parameters.create().apply {
            set("requested", whitelist.size)
            set("working", whitelist)
        }
    }
}

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
    fun count(term: String): Int {
        if (!counts.containsKey(term)) return 0
        return counts[term]
    }
}

fun computeRelevanceModel(docs: List<LTRDoc>, feature: String, depth: Int, flat: Boolean = false, stopwords: Set<String> = inqueryStop): RelevanceModel {
    val fbdocs = docs.sortedByDescending { it.features[feature]!! }.take(depth)

    val rmModel = TObjectDoubleHashMap<String>()
    fbdocs.forEach { doc ->
        val local = doc.freqs.counts
        val length = doc.freqs.length

        val prior = if (flat) 1.0 else doc.features[feature]!!
        local.forEachEntry {term, count ->
            if (stopwords.contains(term)) return@forEachEntry true
            val prob = prior * count.toDouble() / length
            rmModel.adjustOrPutValue(term, prob, prob)
            true
        }
    }

    return RelevanceModel(rmModel)
}

fun normalize(wt: List<WeightedTerm>): List<WeightedTerm> {
    val norm = wt.map { it.score }.sum()
    return wt.map { WeightedTerm(it.score / norm, it.term) }
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "robust")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()
    val qrels = dataset.getQueryJudgments()
    val k = 100
    val rmLambda = 0.2

    dataset.getIndex().use { retr ->
        val lengths = retr.getCollectionStatistics(GExpr("lengths"))
        val env = RREnv(retr)
        forEachQuery(dsName) { q ->
            val queryJudgments = qrels[q.qid]
            val rm = computeRelevanceModel(q.docs, "title-ql-prior", 10)
            val wt = normalize(rm.toTerms(k))
            println(wt.map { it.term })
            println(q.qterms)
            //println(exp.collectTerms())
            val mu = env.mu

            val oq_exprs = q.qterms.map { env.term(it) }
            val rme_exprs = wt.map { env.term(it.term).weighted(it.score) }

            val rm3_expr = env.sum(
                    env.feature("title-ql").weighted(rmLambda),
                    env.sum(rme_exprs).weighted(1.0 - rmLambda).checkNaNs())

            val terms = wt.map { it.term }.toHashSet()
            terms.addAll(q.qterms)
            val qstats = terms
                    .associate { Pair(it, retr.getNodeStatistics(GExpr("counts", it))) }

            val bgs = qstats.mapValues { (_,nstats) -> nstats.cfProbability(lengths) }

            q.docs.forEachIndexed { i, doc ->
                val freqs = doc.freqs

                val length = doc.freqs.length + mu;
                val rmScores = wt.map { (weight, term) ->
                    val c = freqs.count(term)
                    weight * (Math.log((c + mu * bgs[term]!!) / length))
                }.toDoubleArray()

                val rmScore = rmScores.sum()

                doc.features.put("rm-k$k", rmScore)
                val origQ = doc.features["title-ql"]!! * (rmLambda)
                val rmQ = ((1.0 - rmLambda) * rmScore) + origQ
                doc.features.put("rm-k$k", rmQ);

                doc.features.put("rm3-k$k", rm3_expr.eval(doc))
            }

            arrayListOf<String>("rm-k$k", "rm3-k$k", "title-ql").forEach { method ->
                evals.forEach { measure, evalfn ->
                    val score = try {
                        evalfn.evaluate(q.toQResults(method), queryJudgments)
                    } catch (npe: NullPointerException) {
                        System.err.println("NULL in eval...")
                        -Double.MAX_VALUE
                    }
                    ms.push("$measure.$method", score)
                }
            }

            println(ms.means())
        }
    }
    println(ms.means())
}
