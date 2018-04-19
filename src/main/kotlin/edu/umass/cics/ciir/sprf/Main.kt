package edu.umass.cics.ciir.sprf

import edu.umass.cics.ciir.irene.galago.*
import edu.umass.cics.ciir.irene.utils.printer
import gnu.trove.list.array.TDoubleArrayList
import gnu.trove.map.hash.TIntDoubleHashMap
import gnu.trove.map.hash.TObjectDoubleHashMap
import gnu.trove.map.hash.TObjectIntHashMap
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.core.parse.TagTokenizer
import org.lemurproject.galago.utility.MathUtils
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.util.concurrent.ConcurrentHashMap
import kotlin.streams.toList


object BuildFirstRoundRetrieval {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dataset = DataPaths.get(argp.get("dataset", "robust"))

        val qrels = dataset.getQueryJudgments()
        val evals = getEvaluators(listOf("ap", "ndcg"))
        val ms = NamedMeasures()

        val tok = TagTokenizer()
        StreamCreator.openOutputStream("lm.jsonl.gz").printer().use { output ->
            dataset.getIndex().use { retrieval ->
                dataset.getTitleQueries().forEach { qid, qtext ->
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
                        pmake {
                            put("id", doc.name)
                            put("tokenized", doc.terms.joinToString(separator = " "))
                            put("score", it.score)
                            put("rank", it.rank)

                            // fix null doc names.
                            if (doc.name == null) {
                                doc.name = ""
                            }
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
    constructor(p: Parameters) : this(p.getStr("qid"), p.getList("qterms", String::class.java), p.getList("docs", Parameters::class.java).map {FirstPassDoc(it)})
}
data class FirstPassDoc(val id: String, val tokenized: String, val score: Double, val rank: Int) {
    val terms: List<String>
        get() = tokenized.split(" ")

    constructor(p: Parameters) : this(p.getStr("id"), p.getStr("tokenized"), p.getDouble("score"), p.getInt("rank"))

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

val FeatureNormBlacklist = setOf<Int>(11,12,13, 16, 17, 18, 19, 23)

data class ETermFeatures(val term: String, val map: TIntDoubleHashMap) {
    fun norm(fmins: Map<Int, Double>, fmaxs: Map<Int, Double>) {
        map.keys().forEach { fid ->
            if (!FeatureNormBlacklist.contains(fid)) {
                val min = fmins[fid]!!
                val max = fmaxs[fid]!!
                val orig = map[fid]
                val normed = (orig - min) / (max - min)
                if (java.lang.Double.isNaN(normed)) {
                    map.put(fid, 0.5)
                } else {
                    map.put(fid, normed)
                }
            }
        }
    }
}

object GenerateTruthAssociations {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val depth = argp.get("depth", 20)
        val minTermFreq = argp.get("minTermFreq", 3)
        val testTermWeight = argp.get("testTermWeight", 1)

        val dataset = DataPaths.get(argp.get("dataset", "robust"))
        val evals = getEvaluators(listOf("ap", "ndcg"))
        val qrels = dataset.getQueryJudgments()


        dataset.getIndex().use { retr ->
            val dmeasures = FirstPassDoc.load().associate { q ->
                println(q.qid)
                val qj = qrels[q.qid]
                val fDocs = HashMapLM()
                q.docs.take(depth).forEach { doc ->
                    fDocs.pushDoc(doc.terms)
                }
                val candidateTerms = fDocs.termsWithFrequency(minTermFreq)
                val origQ = GExpr("combine")
                origQ.addTerms(q.qterms)
                val gres = retr.transformAndExecuteQuery(origQ.clone())
                val workingP = pmake {
                    val ids = gres.scoredDocuments.map { it.name }
                    set("working", ids)
                    set("requested", ids.size.toLong())
                }
                val origR = gres.toQueryResults()
                val baseline = evals.mapValues { (_, fn) -> fn.evaluate(origR, qj) }

                val truths = ConcurrentHashMap<String, Parameters>()
                candidateTerms.toList().sorted().parallelStream().forEach { term ->
                    if (term !in q.qterms) {
                        //println("$term ${q.qid}")
                        val posQ = GExpr("combine")
                        posQ.add(GExpr.Text(term))
                        posQ.addTerms(q.qterms)
                        posQ.setf("0", testTermWeight)

                        val posR = retr.transformAndExecuteQuery(posQ, workingP.clone()).toQueryResults()

                        val deltaMeasures = evals.mapValues { (measure, fn) ->
                            val posScore = fn.evaluate(posR, qj)
                            val delta = posScore - baseline[measure]!!
                            //println("$term $measure ${delta}")
                            delta
                        }
                        truths.put(term, Parameters.wrap(deltaMeasures))
                    }
                }
                Pair(q.qid, Parameters.wrap(truths))
            }

            StreamCreator.openOutputStream("truths.json.gz").printer().use { out ->
                out.println(Parameters.wrap(dmeasures).toPrettyString())
            }
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

    val dataset = DataPaths.get(argp.get("dataset", "robust"))

    StreamCreator.openOutputStream("features.txt.gz").printer().use { output ->
        dataset.getIndex().use { retr ->
            val bodyStats = retr.getCollectionStatistics(GExpr("lengths"))
            val N = bodyStats.documentCount.toDouble()

            FirstPassDoc.load().forEach { q ->
                val fDocs = HashMapLM()
                q.docs.take(depth).forEach { doc ->
                    fDocs.pushDoc(doc.terms)
                }
                val whitelist = q.docs.take(depth).map { it.id }
                val candidateTerms = fDocs.termsWithFrequency(minTermFreq)

                val qstats = q.qterms.associate {
                    Pair(it, retr.getNodeStatistics(GExpr("counts", it)))
                }

                // Print candidate terms:
                println("${q.qid}: ${candidateTerms.size}")

                val ctfs: List<ETermFeatures> = candidateTerms.parallelStream().map { term ->
                    val fmap = TIntDoubleHashMap()
                    // 2%
                    fmap.put(1,Math.log(fDocs.probability(term)))
                    val stats = retr.getNodeStatistics(GExpr("counts", term))
                    // 11%
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

                    val workingP = pmake {
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

                    val hits = retr.transformAndExecuteQuery(GExpr("bool").push(andQ), workingP.clone()).scoredDocuments.filter { it.score > 0 }.size

                    //println("$term $hits ${q.qterms}")
                    // 6%
                    fmap.put(9, Math.log(hits+0.5))

                    // 7%
                    fmap.put(3, f3ss.logProb(missingProb))
                    // 6%
                    fmap.put(4, f4ss.logProb(missingProb))
                    // 24.6%
                    fmap.put(5, f5ss.logProb(missingProb))
                    // 31.1%
                    fmap.put(6, f6ss.logProb(missingProb))


                    // Diaz, 2016.
                    val idfNum = log2(N + 0.5)
                    val idfDenom = log2(N + 1.0)
                    val idfs = TDoubleArrayList()
                    val dfs = TDoubleArrayList()
                    // rank equivalent to SCS given P_ml is a constant.
                    val SCS = TDoubleArrayList()
                    qstats.values.forEach {
                        val Nt = it.nodeDocumentCount.toDouble();
                        dfs.add(Nt / N)

                        // INQUERY IDF: He & Ounis
                        val idf = (idfNum / Nt) / idfDenom
                        idfs.add(idf)

                        SCS.add(-log2(it.cfProbability(bodyStats)))
                    }
                    val Nt = stats.nodeDocumentCount.toDouble()
                    dfs.add(Nt / N)
                    val idf = (idfNum / Nt) / idfDenom
                    idfs.add(idf)
                    SCS.add(-log2(stats.cfProbability(bodyStats)))

                    val anyTerm = GExpr("count-sum").apply {
                        addTerms(q.qterms)
                        add(GExpr.Text(term))
                    }
                    val anyTermStats = retr.getNodeStatistics(retr.transformQuery(anyTerm, Parameters.create()))

                    fmap.put(11, dfs.max())
                    fmap.put(12, dfs.min())
                    fmap.put(13, dfs.mean())

                    fmap.put(16, idfs.max())
                    fmap.put(17, idfs.min())
                    fmap.put(18, idfs.mean())
                    fmap.put(19, idfs.max() / idfs.min()) // gamma2, He & Ounis

                    fmap.put(20, SCS.sum())
                    fmap.put(21, SCS.mean())

                    // Query Scope, He & Ounis
                    fmap.put(22, -Math.log(anyTermStats.nodeDocumentCount.toDouble() / N))
                    fmap.put(23, anyTermStats.cfProbability(bodyStats))

                    // skip the minimum term distance for now... it's not easy in Galago.
                    ETermFeatures(term, fmap)
                }.toList()

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

                ctfs.forEach { etf ->
                    etf.norm(fmins, fmaxs)

                    // boolean features after normalization.
                    // 10.9%
                    etf.map.put(10, if (inqueryStop.contains(etf.term)) 1.0 else 0.0)

                    // is in the original query?
                    etf.map.put(14, if (q.qterms.contains(etf.term)) 1.0 else 0.0)
                    // Original Query Length, so normalizations can be learned.
                    etf.map.put(15, q.qterms.size.toDouble())

                    val columns = ArrayList<String>()
                    columns.add(q.qid)
                    columns.add(etf.term)
                    etf.map.forEachEntry { fid, score ->
                        columns.add(fid.toString())
                        columns.add(score.toString())
                    }

                    output.println(columns.joinToString("\t"))
                }
            }
        }
    }
}

object UsePredictions {
    @JvmStatic fun main(args: Array<String>) {
        val dataset = DataPaths.Robust
        val qrels = dataset.getQueryJudgments()
        val evals = getEvaluators(listOf("ap", "ndcg"))
        val ms = NamedMeasures()

        val measure = "ndcg";
        val truthsMap = Parameters.parseFile("truths.json.gz")
        val predictions = Parameters.parseFile("predictions.json").getMap("train")
        val testSet = predictions.keys.toCollection(java.util.TreeSet<String>())

        val tok = TagTokenizer()
        dataset.getIndex().use { retrieval ->
            dataset.getTitleQueries()
                    .filterKeys { testSet.contains(it) }
                    .forEach { qid, qtext ->
                        val queryJudgments = qrels[qid]
                        val qterms = tok.tokenize(qtext).terms
                        val lmBaseline = GExpr("combine").apply { addTerms(qterms) }
                        println("$qid $lmBaseline")
                        val gres = retrieval.transformAndExecuteQuery(lmBaseline.clone())
                        val workingP = pmake {
                            val ids = gres.scoredDocuments.map { it.name }
                            set("working", ids)
                            set("requested", ids.size.toLong())
                        }

                        // 0.001 is the best of 0, 0.1, 0.01, 0.001, 0.005, 0.0001
                        listOf(0.0001, 0.005, 0.001).forEach { cutoff ->
                            val tp = truthsMap.getMap(qid)
                            val posTerms = HashSet<String>()
                            tp.keys.forEach { term ->
                                val score = tp.getMap(term).getDouble(measure)
                                if (score > cutoff) {
                                    posTerms.add(term)
                                }
                            }
                            val oracleExpansion = GExpr("combine").apply { addTerms(posTerms.toList()) }
                            val oracleRun = GExpr("combine").apply {
                                addChild(lmBaseline.clone())
                                addChild(oracleExpansion)
                            }
                            val oracleRes = retrieval.transformAndExecuteQuery(oracleRun, workingP.clone())
                            evals.forEach { measure, evalfn ->
                                ms.push("Oracle t=$cutoff $measure", evalfn.evaluate(oracleRes.toQueryResults(), queryJudgments))
                            }
                        }

                        //val expTerms = predictions.getList(qid, String::class.java)!!.toList()

                        //val expRun = GExpr("combine").apply {
                        //    addChild(lmBaseline.clone())
                        //    setf("0", 0.8)
                        //    addChild(GExpr("combine").apply {
                        //        addTerms(expTerms)
                        //    })
                        //    setf("1", 0.2)
                        //}

                        //val eres = retrieval.transformAndExecuteQuery(expRun)
                        //val weres = retrieval.transformAndExecuteQuery(expRun, workingP)

                        evals.forEach { measure, evalfn ->
                            ms.push("LM $measure", evalfn.evaluate(gres.toQueryResults(), queryJudgments))
                            //ms.push("SPRF.flat $measure", evalfn.evaluate(eres.toQueryResults(), queryJudgments))
                            //ms.push("rSPRF.flat $measure", evalfn.evaluate(weres.toQueryResults(), queryJudgments))
                        }
                    }
        }
        println(Parameters.wrap(ms.means()));
    }
}

object BuildRelevanceModels {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val k = argp.get("k", 10)
        val outP = Parameters.create()
        FirstPassDoc.load().forEach { qdocs ->
            println("${qdocs.qid} ${qdocs.qterms}");
            val scores = qdocs.docs.map { it.score }.toDoubleArray()
            val logSum = MathUtils.logSumExp(scores)
            val priors = scores.map { Math.exp(it - logSum) }
            val relevanceModel = TObjectDoubleHashMap<String>()
            qdocs.docs.zip(priors).take(k).forEach{(doc, prior) ->
                val tvec = doc.terms
                val length = tvec.size.toDouble()
                val counts = TObjectIntHashMap<String>()
                tvec.forEach { counts.adjustOrPutValue(it, 1, 1) }
                counts.forEachEntry { term, count ->
                    val prob = prior * count.toDouble() / length
                    relevanceModel.adjustOrPutValue(term, prob, prob)
                    true
                }
            }

            val rmP = Parameters.create()
            relevanceModel.forEachEntry { term, weight ->
                rmP.set(term, weight)
                true
            }
            outP.put(qdocs.qid, rmP)
        }

        StreamCreator.openOutputStream("rm.k$k.json.gz").printer().use {
            it.println(outP.toPrettyString())
        }
    }
}