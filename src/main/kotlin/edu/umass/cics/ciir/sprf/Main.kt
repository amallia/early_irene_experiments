package edu.umass.cics.ciir.sprf

import gnu.trove.list.array.TDoubleArrayList
import gnu.trove.map.hash.TIntDoubleHashMap
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

class HashMapLM {
    val terms = HashMap<String, Int>()
    var length: Double = 0.0

    fun pushDoc(x: List<String>) {
        length += x.size
        x.forEach { term ->
            terms.compute(term, {_, prev -> (prev ?: 0) + 1})
            //terms.adjustOrPutValue(term, 1, 1)
        }
    }
    fun termsWithFrequency(minTF: Int): Set<String> {
        val out = HashSet<String>(terms.size/2)
        terms.forEach { term, count ->
            if (count >= minTF) { out.add(term) }
        }
        return out
    }
    fun probability(term: String): Double = (terms.get(term) ?: 0).toDouble() / length
}

data class ETermFeatures(val term: String, val map: TIntDoubleHashMap) {
    fun norm(fmins: Map<Int, Double>, fmaxs: Map<Int, Double>) {
        map.keys().forEach { fid ->
            val min = fmins[fid]!!
            val max = fmaxs[fid]!!
            val orig = map[fid]
            val normed = (orig - min) / (max - min)
            map.put(fid, normed)
        }
    }
}


fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val depth = argp.get("depth", 20)
    val minTermFreq = argp.get("minTermFreq", 3)
    val uniWindowSize = argp.get("uniWindow", 12)
    val biWindowSize = argp.get("biWindow", 15)
    val missingProb = argp.get("missing", -20.0)

    DataPaths.getRobustIndex().use { retr ->
        val bodyStats = retr.getCollectionStatistics(GExpr("lengths"))

        FirstPassDoc.load().forEach { q ->
            val fDocs = HashMapLM()
            q.docs.take(depth).forEach { doc ->
                fDocs.pushDoc(doc.terms)
            }
            val whitelist = q.docs.take(depth).map { it.id }
            val candidateTerms = fDocs.termsWithFrequency(minTermFreq)

            // Print candidate terms:
            println("${q.qid}: ${candidateTerms.size}")

            val ctfs = candidateTerms.map { term ->
                val fmap = TIntDoubleHashMap()
                fmap.put(1,Math.log(fDocs.probability(term)))
                val stats = retr.getNodeStatistics(GExpr("counts", term))
                fmap.put(2, Math.log(stats.cfProbability(bodyStats)))

                // f3 is co-occurrence with single-query-term
                // f4
                val f3ts = HashSet<List<String>>()
                q.qterms.forEach { q ->
                    val terms = mutableListOf<String>(q, term)
                    terms.sort()
                    f3ts.add(terms)
                }
                val f5ts = HashSet<List<String>>()
                q.qterms.forEachIndexed { i, qi ->
                    q.qterms.indices.forEach { j ->
                        if (i != j) {
                            val terms = mutableListOf<String>(qi, q.qterms[j], term)
                            terms.sort()
                            f5ts.add(terms)
                        }
                    }
                }

                val f3qs = f3ts.map { tokens ->
                    GExpr("uw").apply {
                        setf("default", uniWindowSize)
                        tokens.forEach { addChild(GExpr.Text(it)) }
                    }
                }
                val f5qs = f5ts.map { tokens ->
                    GExpr("uw").apply {
                        setf("default", biWindowSize)
                        tokens.forEach { addChild(GExpr.Text(it)) }
                    }
                }

                val workingP = Parameters.create().apply {
                    set("working", whitelist)
                    set("requested", whitelist.size)
                }
                val f3ss = TDoubleArrayList()
                val f4ss = TDoubleArrayList()
                f3qs.forEach {
                    val bg = retr.getNodeStatistics(retr.transformQuery(it.clone(), Parameters.create()))
                    val fg = retr.transformAndExecuteQuery(GExpr("count-to-score").push(GExpr("count-sum").push(it.clone())), workingP)!!
                    //val bgProb = bg.cfProbability(bodyStats)
                    val fgCount = fg.scoredDocuments.map { it.score }.sum()
                    if (fgCount > 0) {
                        val fgProb = fgCount / fDocs.length
                        // given term, how often does it occur with bg?
                        if (bg.nodeDocumentCount > 0) {
                            val bgProb = bg.nodeDocumentCount.toDouble() / stats.nodeDocumentCount.toDouble()
                            f3ss.add(fgProb)
                            f4ss.add(bgProb)
                        }
                        //println("$it $fgProb $bgProb")
                    }
                }

                val f5ss = TDoubleArrayList()
                val f6ss = TDoubleArrayList()
                f5qs.forEach {
                    val bg = retr.getNodeStatistics(retr.transformQuery(it.clone(), Parameters.create()))
                    val fg = retr.transformAndExecuteQuery(GExpr("count-to-score").push(GExpr("count-sum").push(it.clone())), workingP.clone())!!
                    //val bgProb = bg.cfProbability(bodyStats)
                    val fgCount = fg.scoredDocuments.map { it.score }.sum()
                    if (fgCount > 0) {
                        val fgProb = fgCount / fDocs.length
                        // given term, how often does it occur with bg?
                        if (bg.nodeDocumentCount > 0) {
                            val bgProb = bg.nodeDocumentCount.toDouble() / stats.nodeDocumentCount.toDouble()
                            f5ss.add(fgProb)
                            f6ss.add(bgProb)
                        }
                        //println("$it $fgProb $bgProb")
                    }
                }

                // f9
                val andQ = GExpr("band")
                q.qterms.forEach { andQ.addChild(GExpr.Text(it)) }
                andQ.addChild(GExpr.Text(term))

                val hits = retr.transformAndExecuteQuery(GExpr("bool").push(andQ), workingP.clone()).scoredDocuments.size

                fmap.put(9, Math.log(hits+0.5))

                fmap.put(3, f3ss.logProb(missingProb))
                fmap.put(4, f4ss.logProb(missingProb))
                fmap.put(5, f5ss.logProb(missingProb))
                fmap.put(6, f6ss.logProb(missingProb))


                // skip the minimum term distance for now... it's not easy in Galago.
                ETermFeatures(term, fmap)
            }

            // Now max/min normalize the features per query:
            val fstats = HashMap<Int, TDoubleArrayList>()
            ctfs.forEach {
                it.map.forEachEntry {fid,fval ->
                    fstats.computeIfAbsent(fid, {TDoubleArrayList()}).add(fval)
                    true
                }
            }

            val fmins = fstats.mapValues { (_,arr) -> arr.min() }
            val fmaxs = fstats.mapValues { (_,arr) -> arr.max() }

            ctfs.forEach { it.norm(fmins, fmaxs) }

            println("NORMED: ${ctfs.size}")

        }
    }


}

fun TDoubleArrayList.logProb(orElse: Double): Double {
    if (this.size() == 0) return orElse;
    return Math.log(this.sum() / this.size().toDouble())
}

