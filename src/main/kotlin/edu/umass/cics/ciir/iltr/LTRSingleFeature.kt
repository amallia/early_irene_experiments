package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.*
import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.core.eval.EvalDoc
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import org.lemurproject.galago.utility.lists.Ranked

/**
 * @author jfoley
 */

class LTRDocByFeature(private val feature: String, val doc: LTRDoc, rank: Int, score: Double) : EvalDoc, Ranked(rank, score) {
    override fun getRank(): Int = rank
    override fun getScore(): Double = score
    override fun getName(): String = doc.name
    override fun clone(score: Double): LTRDocByFeature = LTRDocByFeature(feature, doc, rank, score)
    constructor(feature: String, doc: LTRDoc) : this(feature, doc, -1, doc.features[feature] ?: error("Feature $feature not available!"))
}


interface ILTRDocField {
    val name: String
    val text: String
    val tokenizer: GenericTokenizer
    val terms: List<String>
    val freqs: BagOfWords
    val length: Int
    val uniqTerms: Int
    val termSet: Set<String>
    fun toEntry(): Pair<String, ILTRDocField> = Pair(name, this)
    fun count(term: String): Int
}

data class LTREmptyDocField(override val name: String) : ILTRDocField {
    override val text: String = ""
    override val tokenizer: GenericTokenizer = WhitespaceTokenizer()
    override val terms: List<String> = emptyList()
    override val freqs: BagOfWords = BagOfWords(emptyList())
    override val length: Int = 1
    override val uniqTerms: Int = 0
    override val termSet: Set<String> = emptySet()
    override fun count(term: String): Int = 0
}

data class LTRDocField(override val name: String, override val text: String, override val tokenizer: GenericTokenizer = WhitespaceTokenizer()) : ILTRDocField {
    override val terms: List<String> by lazy { tokenizer.tokenize(text, name) }
    override val freqs: BagOfWords by lazy { BagOfWords(terms) }
    override val length: Int get() = terms.size
    override val uniqTerms: Int get() = freqs.counts.size()
    override fun count(term: String): Int = freqs.count(term)
    override val termSet: Set<String> get() = freqs.counts.keySet()
}


data class LTRDoc(val name: String, val features: HashMap<String, Double>, val rank: Int, val fields: HashMap<String,ILTRDocField>, var defaultField: String = "document") {
    fun field(field: String): ILTRDocField = fields[field] ?: error("No such field: $field in $this.")
    fun terms(field: String) = field(field).terms
    fun freqs(field: String) = field(field).freqs

    constructor(name: String, text: String, tokenizer: GenericTokenizer = WhitespaceTokenizer()): this(name, hashMapOf(), -1, hashMapOf(LTRDocField("document", text, tokenizer).toEntry()), "document")

    constructor(p: Parameters): this(p.getStr("id"),
            hashMapOf(
                    Pair("title-ql", p.getDouble("title-ql")),
                    Pair("title-ql-prior", p.getDouble("title-ql-prior"))),
            p.getInt("rank"),
            hashMapOf(LTRDocField("document", p.getStr("tokenized")).toEntry()))

    fun toJSONFeatures(qrels: QueryJudgments, qid: String) = pmake {
        set("label", qrels[name])
        set("qid", qid)
        set("features", Parameters.wrap(features))
        set("name", name)
    }
}

data class LTRQuery(val qid: String, val qtext: String, val qterms: List<String>, val docs: List<LTRDoc>) {
    fun ranked(ftr: String): ArrayList<LTRDocByFeature> = docs.mapTo(ArrayList<LTRDocByFeature>(docs.size)) { LTRDocByFeature(ftr, it) }
    fun ranked(expr: RRExpr): ArrayList<LTRDocByFeature> = docs.mapTo(ArrayList<LTRDocByFeature>(docs.size)) { LTRDocByFeature("RRExpr", it, -1, expr.eval(it)) }
    fun toQResults(ftr: String): QueryResults {
        val ranked = ranked(ftr)
        Ranked.setRanksByScore(ranked)
        return QueryResults(ranked)
    }
    fun toQResults(rrExpr: RRExpr): QueryResults {
        val ranked = ranked(rrExpr)
        Ranked.setRanksByScore(ranked)
        return QueryResults(ranked)
    }

    fun whitelistParameters(): Parameters {
        val whitelist = docs.map { it.name }.toList()
        return pmake {
            set("requested", whitelist.size)
            set("working", whitelist)
        }
    }

    fun toJSONFeatures(qrels: QueryJudgments) = docs.map { it.toJSONFeatures(qrels, qid) }
}

fun forEachQuery(dsName: String, doFn: (LTRQuery) -> Unit) {
    StreamCreator.openInputStream("$dsName.qlpool.jsonl.gz").reader().useLines { lines ->
        lines.map { Parameters.parseStringOrDie(it) }.forEach { qjson ->
            val qid = qjson.getStr("qid")
            val qtext = qjson.getStr("qtext")
            val qterms = qjson.getAsList("qterms", String::class.java)

            val docs = qjson.getAsList("docs", Parameters::class.java).map { LTRDoc(it) }

            doFn(LTRQuery(qid, qtext, qterms, docs))
        }
    }
}


fun List<WeightedTerm>.normalized(): List<WeightedTerm> {
    val total = this.sumByDouble { it.score }
    return this.map { WeightedTerm(it.score / total, it.term) }
}

class BagOfWords(terms: List<String>) {
    val counts = TObjectIntHashMap<String>()
    val length = terms.size.toDouble()
    val l2norm: Double by lazy {
        var sumSq = 0.0
        counts.forEachValue { c ->
            sumSq += c*c
            true
        }
        Math.sqrt(sumSq)
    }
    init {
        terms.forEach { counts.adjustOrPutValue(it, 1, 1) }
    }
    fun prob(term: String): Double = counts.get(term) / length
    fun count(term: String): Int {
        if (!counts.containsKey(term)) return 0
        return counts[term]
    }

}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "robust")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()
    val qrels = dataset.getQueryJudgments()
    val fbTerms = 100
    val rmLambda = 0.2

    val lang = IreneQueryLanguage()
    lang.defaultField = "document"

    StreamCreator.openOutputStream("$dsName.features.jsonl.gz").printer().use { out ->
        dataset.getIndex().use { retr ->
            val env = RRGalagoEnv(retr)

            dataset.getBM25B()?.let { env.defaultBM25b = it }
            dataset.getBM25K()?.let { env.defaultBM25k = it }

            forEachQuery(dsName) { q ->
                val queryJudgments = qrels[q.qid]
                println(q.qterms)

                val feature_exprs = hashMapOf<String, RRExpr>(
                        Pair("bm25", env.bm25(q.qterms)),
                        Pair("LM-dir", QueryLikelihood(q.qterms).toRRExpr(env)),
                        Pair("LM-abs", env.mean(q.qterms.map { RRAbsoluteDiscounting(env, it) })),
                        Pair("docinfo", RRDocInfoQuotient(env)),
                        Pair("sdm", SequentialDependenceModel(q.qterms).toRRExpr(env)),
                        Pair("sdm-stop", SequentialDependenceModel(q.qterms, stopwords=inqueryStop).toRRExpr(env)),
                        Pair("avgwl", RRAvgWordLength(env)),
                        Pair("docl", RRDocLength(env)),
                        Pair("meantp", env.mean(q.qterms.map { RRTermPosition(env, it) })),
                        Pair("jaccard-stop", RRJaccardSimilarity(env, inqueryStop)),
                        Pair("length", RRDocLength(env))
                )

                arrayListOf<Int>(5, 10, 25).forEach { fbDocs ->
                    val rm = env.computeRelevanceModel(q.docs, "title-ql-prior", fbDocs)
                    val wt = rm.toTerms(fbTerms)
                    val rmeExpr = rm.toQExpr(fbTerms).toRRExpr(env)
                    feature_exprs.put("rm3-k$fbDocs", env.feature("title-ql").mixed(rmLambda, rmeExpr))
                    feature_exprs.put("jaccard-rm3-k$fbDocs", RRJaccardSimilarity(env, wt.map { it.term }.toSet()))
                }

                q.docs.forEachIndexed { _, doc ->
                    feature_exprs.forEach { fname, fexpr ->
                        doc.features.put(fname, fexpr.eval(doc))
                    }
                }

                arrayListOf<String>("rm3-k5", "rm3-k10", "rm3-k25", "bm25", "title-ql", "LM-abs", "LM-dir").forEach { method ->
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

                q.toJSONFeatures(queryJudgments).forEach { out.println(it) }
                println(ms.means())
            }
        }
    }
    println(ms.means())
}
