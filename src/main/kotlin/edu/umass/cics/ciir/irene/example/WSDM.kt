package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.iltr.LTRDoc
import edu.umass.cics.ciir.iltr.toRRExpr
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.learning.*
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.collections.HashSet

typealias IVector = edu.umass.cics.ciir.learning.Vector

/**
 * @author jfoley
 */
object PrepareMSNQLogDB {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val columns = listOf("Time", "Query", "QueryID", "SessionID", "ResultCount")
    val dir = File("/mnt/scratch/jfoley/msn-qlog/")
    val NLines = 14921286
    val NQ = NLines-1
    val log = File(dir, "srfp20060501-20060531.queries.txt.gz")

    val params = IndexParams().apply {
        withPath(File(dir, "msn-qlog.irene2"))
        defaultField = "query"
        create()
    }

    @JvmStatic fun main(args: Array<String>) {
        IreneIndexer(params).use { indexer ->
            log.smartReader().useLines { lines ->
                val iterLines = lines.iterator()
                val header = iterLines.next().split("\t")
                val qIndex = header.indexOf("Query")
                if (qIndex < 0) error("Couldn't find `Query' heading in $header")

                val msg = CountingDebouncer(NQ.toLong())
                while (iterLines.hasNext()) {
                    val cols = iterLines.next().split("\t")
                    val time = cols[0]
                    val queryText = cols[1]
                    val qid = cols[2]
                    val session = cols[3]
                    val resultCount = cols[4].toInt()

                    val ldt = LocalDateTime.parse(time, formatter)
                    val millis = ldt.atOffset(ZoneOffset.UTC).toEpochSecond()

                    indexer.doc {
                        setId(qid)
                        setStringField("session", session)
                        setDenseIntField("resultCount", resultCount)
                        setDenseLongField("time", millis)
                        setTextField(params.defaultField, queryText)
                    }

                    msg.incr()?.let { upd ->
                        println(upd)
                    }
                }
            }
        }
    }
}

object GoogleNGrams {
    val dir = when(IRDataset.host) {
        "oakey" -> File("/mnt/scratch/jfoley/google-n-grams-T13")
        "sydney" -> File("/mnt/nfs/collections/google-n-grams-T13")
        else -> notImpl(IRDataset.host)
    }
    val numTerms = 1_024_908_267_229L
    val numUnigramLines = 13_588_391

    fun pullUnigramStats(request: Set<String>): Map<String, Long> {
        val terms = request.toHashSet()
        val outputFreqs = HashMap<String, Long>()

        val reader = File(dir, "1gms/vocab.gz").smartReader()
        val msg = CountingDebouncer(total=numUnigramLines.toLong())
        while(terms.isNotEmpty()) {
            val (term, countStr) = reader.readLine()?.splitAt('\t') ?: break
            if (terms.remove(term)) {
                outputFreqs.put(term, countStr.toLong())
            }
            msg.incr()?.let { upd ->
                println("pullUnigramStats ${outputFreqs.size}/${request.size} ... $upd")
            }
        }

        return outputFreqs
    }

    const val numBigramLines = 10_000_000L * 31
    const val bigramTotal = 9.10884463583e+11

    fun pullBigramStats(request: Set<Pair<String, String>>): Map<Pair<String,String>, Long> {
        val terms = request.toHashSet()
        val outputFreqs = HashMap<Pair<String, String>, Long>()

        val inputs =  File(dir, "2gms").listFiles().filter { it.name.startsWith("2gm-00") && it.name.endsWith(".gz") }.toCollection(LinkedList())
        println(inputs)
        //val inputs = inputFiles.map { it.smartReader() }.toCollection(LinkedList())

        val msg = CountingDebouncer(total= numBigramLines)
        while(terms.isNotEmpty() && inputs.isNotEmpty()) {
            val inputF = inputs.pop() ?: break
            inputF.smartReader().use { reader ->
                while (terms.isNotEmpty()) {
                    val (phrase, countStr) = reader.readLine()?.splitAt('\t') ?: break
                    val bigram = phrase.splitAt(' ') ?: continue
                    if (terms.remove(bigram)) {
                        outputFreqs.put(bigram, countStr.toLong())
                    }
                    msg.incr()?.let { upd ->
                        println("pullBigramStats ${outputFreqs.size}/${request.size} ... $upd")
                    }
                }
            }
        }


        return outputFreqs
    }
}

fun proxQuery(terms: List<String>, field: String? = null, statsField: String? = null) = UnorderedWindowExpr(terms.map { TextExpr(it, field, statsField) })

object CollectWSDMFeatures {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dsName = argp.get("dataset", "robust")
        val dataset = DataPaths.get(dsName)
        val output = File("$dsName.wsdmf.json")

        WikiSource().getIreneIndex().use { wiki ->
            IreneIndex(PrepareMSNQLogDB.params).use { msn ->
                dataset.getIreneIndex().use { index ->
                    val uniqTerms = HashSet<String>()
                    val uniqPairs = HashSet<Pair<String,String>>()
                    (dataset.title_qs.values + dataset.desc_qs.values).forEach { query ->
                        val qterms = index.tokenize(query)
                        uniqTerms.addAll(qterms)
                        qterms.forEachSeqPair { lhs, rhs ->
                            uniqPairs.add(Pair(lhs, rhs))
                        }
                    }

                    val gTerms = GoogleNGrams.pullUnigramStats(uniqTerms)
                    val swappedOrder = uniqPairs.flatMapTo(HashSet()) { listOf(it, Pair(it.second, it.first)) }
                    val bgTerms = GoogleNGrams.pullBigramStats(swappedOrder)
                    println(gTerms)

                    val uniF = uniqTerms.pmap { term ->
                        val collection = index.getStats(term)
                        val wikiTitles = wiki.getStats(term, "title")
                        val wikiExact = wiki.getStats(generateExactMatchQuery(listOf(term), "title"))
                        val msnCount = msn.getStats(term)
                        val msnExact = msn.getStats(generateExactMatchQuery(listOf(term)))
                        val gfe = gTerms[term] ?: 0L

                        val features = Parameters.parseArray(
                                "cf", collection.nonzeroCountProbability(), "df", collection.binaryProbability(),
                                "wt.cf", wikiTitles.nonzeroCountProbability(), "wt.df", wikiTitles.binaryProbability(),
                                "wt.exact", wikiExact.cf,
                                "msn.cf", msnCount.nonzeroCountProbability(), "msn.df", msnCount.binaryProbability(),
                                "msn.exact", msnExact.cf,
                                "gfe", gfe / GoogleNGrams.numTerms.toDouble(),
                                "inqueryStop", inqueryStop.contains(term))
                        println("$term $features")

                        Pair(term, features)
                    }.associate { it }

                    val msg = CountingDebouncer(uniqPairs.size.toLong()*2)
                    val uwF = uniqPairs.pmap { (lhs, rhs) ->
                        val collection = index.getStats(proxQuery(listOf(lhs, rhs)))
                        val wikiTitles = wiki.getStats(proxQuery(listOf(lhs, rhs), "title"))
                        val wikiExact = wiki.getStats(generateExactMatchQuery(listOf(lhs, rhs), "title"))
                        val msnCount = msn.getStats(proxQuery(listOf(lhs, rhs)))
                        val msnExact = msn.getStats(generateExactMatchQuery(listOf(lhs, rhs)))
                        var gfe = (bgTerms[Pair(lhs, rhs)] ?: 0L) + (bgTerms[Pair(rhs, lhs)] ?: 0L)
                        if (gfe == 0L)
                            gfe = minOf(gTerms[lhs] ?: gTerms[rhs] ?: 0L, gTerms[rhs] ?: gTerms[lhs] ?: 0L)

                        val features = Parameters.parseArray(
                                "cf", collection.nonzeroCountProbability(), "df", collection.binaryProbability(),
                                "wt.cf", wikiTitles.nonzeroCountProbability(), "wt.df", wikiTitles.binaryProbability(),
                                "wt.exact", wikiExact.cf,
                                "msn.cf", msnCount.nonzeroCountProbability(), "msn.df", msnCount.binaryProbability(),
                                "msn.exact", msnExact.cf,
                                "gfe", gfe / GoogleNGrams.bigramTotal,
                                "inqueryStop", inqueryStop.contains(lhs) && inqueryStop.contains(rhs))

                        msg.incr()?.let { upd ->
                            println("uw: $lhs $rhs $features")
                            println(upd)
                        }

                        Pair("$lhs $rhs", features)
                    }.associate { it }

                    val biF = uniqPairs.pmap { (lhs, rhs) ->
                        val collection = index.getStats(phraseQuery(listOf(lhs, rhs)))
                        val wikiTitles = wiki.getStats(phraseQuery(listOf(lhs, rhs), "title"))
                        val wikiExact = wiki.getStats(generateExactMatchQuery(listOf(lhs, rhs), "title"))
                        val msnCount = msn.getStats(phraseQuery(listOf(lhs, rhs)))
                        val msnExact = msn.getStats(generateExactMatchQuery(listOf(lhs, rhs)))
                        val gfe = bgTerms[Pair(lhs, rhs)] ?: 0L

                        val features = Parameters.parseArray(
                                "cf", collection.nonzeroCountProbability(), "df", collection.binaryProbability(),
                                "wt.cf", wikiTitles.nonzeroCountProbability(), "wt.df", wikiTitles.binaryProbability(),
                                "wt.exact", wikiExact.cf,
                                "msn.cf", msnCount.nonzeroCountProbability(), "msn.df", msnCount.binaryProbability(),
                                "msn.exact", msnExact.cf,
                                "gfe", gfe / GoogleNGrams.bigramTotal,
                                "inqueryStop", inqueryStop.contains(lhs) && inqueryStop.contains(rhs))
                        msg.incr()?.let { upd ->
                            println("bi: $lhs $rhs $features")
                            println(upd)
                        }

                        Pair("$lhs $rhs", features)
                    }.associate { it }

                    val features = Parameters.create()
                    features.put("t", Parameters.wrap(uniF))
                    features.put("od", Parameters.wrap(biF))
                    features.put("uw", Parameters.wrap(uwF))

                    output.smartPrint { writer ->
                        writer.println(features.toPrettyString())
                    }
                }
            }
        }
    }
}

val wsdmFeatureNames = listOf(
        // SDM-p
        "t", "od", "bi",
        // WSDM-p
        "cf", "df", "wt.cf", "wt.df", "wt.exact", "msn.cf", "msn.df", "msn.exact", "gfe", "inqueryStop")

fun wsdmFeatureVector(p: Parameters): SimpleDenseVector {
    val out = SimpleDenseVector(wsdmFeatureNames.size)
    p.entries.forEach { (name, value) ->
        val i = wsdmFeatureNames.indexOf(name)
        if (i == -1) error("Bad feature: $name")
        val y = when (value) {
            is Boolean -> if (value) 1.0 else 0.0
            is Number -> value.toDouble()
            is String -> 0.0
            else -> error("value=$value for $name")
        }
        out[i] = y
    }
    return out
}

data class WSDMComponent(val features: IVector, val score: Double) {
    fun weight(w: IVector) = w.dotp(features)
}

data class WSDMCliqueEval(val components: List<WSDMComponent> = emptyList()) {
    fun eval(w: IVector): Double {
        if (components.isEmpty()) return 0.0

        val weights = components.map { it.weight(w) }.normalize()

        return weights.zip(components).sumByDouble { (normW, base) ->
            normW * base.score
        }
    }
}

data class WSDMTopLevel(val relevant: Boolean, val t: WSDMCliqueEval, val od: WSDMCliqueEval, val uw: WSDMCliqueEval) {
    fun eval(w: IVector): Double {
        val tw = w[0]
        val odw = w[1]
        val uww  = w[2]

        return tw * t.eval(w) + odw * od.eval(w) + uww * uw.eval(w)
    }
}

data class SimplePrediction(override val score: Double, override val correct: Boolean): Prediction { }

object LearnWSDMParameters {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dsName = argp.get("dataset", "robust")
        val dataset = DataPaths.get(dsName)
        val features = Parameters.parseFile(File("$dsName.wsdmf.json"))

        val unF = features.getMap("t").entries.map { (term, kv) ->
            Pair(term, wsdmFeatureVector(kv as Parameters))
        }.associate { it }
        val odF = features.getMap("od").entries.map { (terms, kv) ->
            Pair(terms.splitAt(' '), wsdmFeatureVector(kv as Parameters))
        }.associate { it }
        val uwF = features.getMap("uw").entries.map { (terms, kv) ->
            Pair(terms.splitAt(' '), wsdmFeatureVector(kv as Parameters))
        }.associate { it }

        println(unF["the"])

        val qrels = dataset.qrels
        val queries = dataset.title_qs

        // hack for old index
        dataset.getIreneIndex().use { index ->
            val fields = setOf(index.idFieldName, index.defaultField)
            val env = index.env
            val ramPool = queries.entries.map { (qid, qtext) ->
                val judgments = qrels[qid] ?: return@map Pair(qid, emptyList<WSDMTopLevel>())
                val qterms = index.tokenize(qtext)
                val sdmQ = SequentialDependenceModel(qterms)
                println("$qid .. $qterms")
                val results = index.search(sdmQ, 300)
                println("$qid .. $qterms .. ${results.totalHits}")

                val pool = results.scoreDocs.mapNotNull { index.reader.document(it.doc, fields) }.associateByTo(HashMap()) { it[index.idFieldName] }
                val missingJudged = judgments.keys.filterNot { pool.containsKey(it) }
                for (name in missingJudged) {
                    if (name == null) continue
                    val num = index.documentById(name) ?: continue
                    val json = index.reader.document(num, fields) ?: continue
                    pool[name] = json
                }

                println("$qid ${pool.size}")

                val wsdm = listOf(
                        qterms.map { Pair(unF[it]!!, DirQLExpr(TextExpr(it)).toRRExpr(env)) },
                        qterms.mapEachSeqPair { lhs, rhs ->
                            Pair(odF[Pair(lhs, rhs)]!!, DirQLExpr(OrderedWindowExpr(listOf(TextExpr(lhs), TextExpr(rhs)))).toRRExpr(env))
                        },
                        qterms.mapEachSeqPair { lhs, rhs ->
                            Pair(uwF[Pair(lhs, rhs)]!!, DirQLExpr(UnorderedWindowExpr(listOf(TextExpr(lhs), TextExpr(rhs)))).toRRExpr(env))
                        })


                val docs = pool.values.map { p ->
                    val body = p.getField(index.defaultField)?.stringValue() ?: ""
                    val name = p[index.idFieldName]
                    val scorable = LTRDoc(name, body, index.defaultField, index.tokenizer)

                    val parts = wsdm.map { wsdmx ->
                        WSDMCliqueEval(wsdmx.map { (fv, expr) ->
                            WSDMComponent(fv, expr.eval(scorable))
                        })
                    }
                    WSDMTopLevel(qrels[qid].isRelevant(name), parts[0], parts[1], parts[2])
                }


                Pair(qid, docs)
            }.associate { it }

            val nRel = ramPool.mapValues { (_, v) -> v.count { it.relevant } }

            val opt = object : GenericOptimizable(wsdmFeatureNames.size, "AP") {
                override fun beginOptimizing(fid: Int, weights: DoubleArray) {
                }
                override fun score(weights: DoubleArray): Double {
                    assert(weights.size == wsdmFeatureNames.size)
                    val vec = SimpleDenseVector(weights.size, weights)

                    val ss = StreamingStats()
                    ramPool.forEach { qid, xs ->
                        val preds = xs.map { x -> SimplePrediction(x.eval(vec), x.relevant) }
                        ss.push(computeAP(preds.sorted(), nRel[qid] ?: 0))
                    }
                    return ss.mean
                }
            }

            val ca = GenericOptimizer(opt)
            ca.nRestart = 1
            ca.nMaxIteration = 10
            ca.learn()

            val model = wsdmFeatureNames.zip(ca.weight.toList()).associate { it }
            println(model)
            File("$dsName.model.json").smartPrint { out ->
                out.println(Parameters.wrap(model).toPrettyString())
            }
        }

    }
}
