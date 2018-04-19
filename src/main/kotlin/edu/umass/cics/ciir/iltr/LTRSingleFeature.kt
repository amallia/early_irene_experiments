package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.ltr.RREnv
import edu.umass.cics.ciir.irene.ltr.RRExpr
import edu.umass.cics.ciir.irene.ltr.WeightedTerm
import edu.umass.cics.ciir.irene.ltr.toRRExpr
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import edu.umass.cics.ciir.irene.scoring.LTRDocField
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.galago.pmake
import edu.umass.cics.ciir.sprf.reader
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
    constructor(feature: String, doc: LTRDoc) : this(feature, doc, -1, doc.features[feature] ?: -Double.MAX_VALUE)
}

data class LTRQuery(val qid: String, val qtext: String, val qterms: List<String>, val docs: List<LTRDoc>) {
    fun ranked(ftr: String): ArrayList<LTRDocByFeature> = docs.mapTo(ArrayList<LTRDocByFeature>(docs.size)) { LTRDocByFeature(ftr, it) }
    fun ranked(expr: RRExpr): ArrayList<LTRDocByFeature> = docs.mapTo(ArrayList<LTRDocByFeature>(docs.size)) { LTRDocByFeature("RRExpr", it, -1, expr.eval(it)) }
    fun toQResults(ftr: String): QueryResults {
        val ranked = ranked(ftr)
        Ranked.setRanksByScore(ranked)
        return QueryResults(ranked)
    }
    fun toQResults(env: RREnv, qExpr: QExpr): QueryResults = toQResults(qExpr.toRRExpr(env))
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
    fun toJSONDocs() = pmake {
        set("qid", qid)
        set("qtext", qtext)
        set("qterms", qterms)
        set("docs", docs.map { it.toJSONDoc() })
    }

}

fun LTRDocFromParameters(p: Parameters) = LTRDoc(p.getStr("id"),
        hashMapOf(
                Pair("title-ql", p.getDouble("title-ql")),
                Pair("title-ql-prior", p.getDouble("title-ql-prior"))),
        hashMapOf(LTRDocField("document", p.getStr("tokenized")).toEntry()))

fun forEachQuery(dsName: String, doFn: (LTRQuery) -> Unit) {
    StreamCreator.openInputStream("$dsName.qlpool.jsonl.gz").reader().useLines { lines ->
        lines.map { Parameters.parseStringOrDie(it) }.forEach { qjson ->
            val qid = qjson.getStr("qid")
            val qtext = qjson.getStr("qtext")
            val qterms = qjson.getAsList("qterms", String::class.java)

            val docs = qjson.getAsList("docs", Parameters::class.java).map { LTRDocFromParameters(it) }

            doFn(LTRQuery(qid, qtext, qterms, docs))
        }
    }
}


fun List<WeightedTerm>.normalized(): List<WeightedTerm> {
    val total = this.sumByDouble { it.score }
    return this.map { WeightedTerm(it.score / total, it.term) }
}


