package edu.umass.cics.ciir.sprf

import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.core.parse.TagTokenizer
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.*

fun OutputStream.writer(): PrintWriter = PrintWriter(OutputStreamWriter(this, Charsets.UTF_8))
fun InputStream.reader(): BufferedReader = BufferedReader(InputStreamReader(this, Charsets.UTF_8))

object BuildFirstRoundRetrieval {
    fun main(args: Array<String>) {
        val qrels = DataPaths.getQueryJudgments()
        val evals = getEvaluators(listOf("ap", "ndcg"))
        val ms = NamedMeasures()

        val tok = TagTokenizer()
        StreamCreator.openOutputStream("lm.jsonl.gz").writer().use { output ->
            DataPaths.getRobustIndex().use { retrieval ->
                DataPaths.getTitleQueries().forEach { qid, qtext ->
                    val queryJudgments = qrels[qid]
                    val qterms = tok.tokenize(qtext).terms
                    val lmBaseline = GExpr("combine").apply { addTerms(qterms) }
                    println("$qid $lmBaseline")

                    val qjson = Parameters.create()
                    qjson.put("qid", qid)
                    qjson.put("qterms", qterms)

                    val gres = retrieval.transformAndExecuteQuery(lmBaseline)

                    val first50 = gres.scoredDocuments.take(50)
                    qjson.put("docs", first50.map {
                        val doc = retrieval.getDocument(it.documentName, Document.DocumentComponents.JustTerms)
                        Parameters.create().apply {
                            put("id", doc.name)
                            put("tokenized", doc.terms.joinToString(separator = " "))
                            put("score", it.score)
                            put("rank", it.rank)
                        }
                    })

                    evals.forEach { measure, evalfn ->
                        val score = evalfn.evaluate(gres.toQueryResults(), queryJudgments)
                        ms.push("LM $measure", score)
                        qjson.put(measure, score)
                    }

                    output.println(qjson)
                }
            }
        }
        println(Parameters.wrap(ms.means()));
    }
}

data class FirstPassQuery(val qid: String, val qterms: List<String>, val docs: List<FirstPassDoc>) {
    constructor(p: Parameters) : this(p.getString("qid"), p.getList("qterms", String::class.java), p.getList("docs", Parameters::class.java).map {FirstPassDoc(it)})
}
data class FirstPassDoc(val id: String, val tokenized: String, val score: Double, val rank: Int) {
    val terms: List<String>
        get() = tokenized.split(" ")

    constructor(p: Parameters) : this(p.getString("id"), p.getString("tokenized"), p.getDouble("score"), p.getInt("rank"))

    companion object {
        fun load(path: String="lm.jsonl.gz"): List<FirstPassQuery> {
            StreamCreator.openInputStream(path).reader().useLines {
                lines -> return lines.map {
                    val itP = Parameters.parseStringOrDie(it)
                    FirstPassQuery(itP)
                }.toList()
            }
        }
    }
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val depth = argp.get("depth", 20)
    val minTermFreq = argp.get("minTermFreq", 3)

    FirstPassDoc.load().forEach { q ->
        val allTerms = TObjectIntHashMap<String>()
        q.docs.take(depth).forEach { doc ->
            doc.terms.forEach { term ->
                allTerms.adjustOrPutValue(term, 1, 1)
            }
        }

        val candidateTerms = HashSet<String>(allTerms.size())
        allTerms.forEachEntry { term, count ->
            if (count >= minTermFreq) { candidateTerms.add(term) }
            true
        }

        // Print candidate terms:
        println("${q.qid}: ${candidateTerms.size}")
    }


}
