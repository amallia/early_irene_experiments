package edu.umass.cics.ciir.sprf

import org.lemurproject.galago.core.parse.TagTokenizer
import org.lemurproject.galago.utility.Parameters

fun main(args: Array<String>) {
    val qrels = DataPaths.getQueryJudgments()
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()

    val tok = TagTokenizer()
    DataPaths.getRobustIndex().use { retrieval ->
        DataPaths.getTitleQueries().forEach {qid, qtext ->
            val queryJudgments = qrels[qid]
            val qterms = tok.tokenize(qtext).terms
            val lmBaseline = GExpr("combine").apply { addTerms(qterms) }
            println("$qid $lmBaseline")

            val gres = retrieval.transformAndExecuteQuery(lmBaseline)

            evals.forEach{ measure, evalfn ->
                val score = evalfn.evaluate(gres.toQueryResults(), queryJudgments)
                ms.push("LM $measure", score)
            }
        }
    }
    println(Parameters.wrap(ms.means()));
}
