package edu.umass.cics.ciir.sprf

import edu.umass.cics.ciir.irene.utils.StreamingStats
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.irene.galago.*
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.scoring.forEach
import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.umass.cics.ciir.irene.utils.timed
import org.apache.lucene.benchmark.byTask.feeds.DocData
import org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException
import org.apache.lucene.benchmark.byTask.feeds.TrecContentSource
import org.apache.lucene.benchmark.byTask.utils.Config
import org.apache.lucene.index.Term
import org.lemurproject.galago.core.eval.QueryJudgments
import java.io.File
import java.util.*

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val tcs = TrecContentSource()
    tcs.config = Config(Properties().apply {
        setProperty("tests.verbose", "false")
        setProperty("content.source.excludeIteration", "true")
        setProperty("content.source.forever", "false")
        setProperty("docs.dir", when(IRDataset.host) {
            "gob" -> "/media/jfoley/flash/raw/robust04/data/"
            "oakey" -> "/mnt/scratch/jfoley/robust04raw/"
            else -> error("Robust raw path needed.")
        })
    })

    val msg = CountingDebouncer(527_719)
    IreneIndexer.build {
        withPath(File("/mnt/scratch/jfoley/robust.irene2"))
        create()
    }.use { writer ->
        while(true) {
            var doc = DocData()
            try {
                doc = tcs.getNextDocData(doc)
            } catch (e : NoMoreDataException) {
                break
            } catch (e : Throwable) {
                e.printStackTrace(System.err)
            }

            writer.doc {
                setId(doc.name)
                setTextField("body", doc.body, true)
                maybeTextField("title", doc.title, true)
                maybeTextField("date", doc.date, true)
            }
            msg.incr()?.let { upd ->
                println("Indexing robust.irene2 $upd")
            }
        }
    }
}

object CreateBigramRobust {
    @JvmStatic fun main(args: Array<String>) {
        val tcs = TrecContentSource()
        tcs.config = Config(Properties().apply {
            setProperty("tests.verbose", "false")
            setProperty("content.source.excludeIteration", "true")
            setProperty("content.source.forever", "false")
            setProperty("docs.dir", when(IRDataset.host) {
                "gob" -> "/media/jfoley/flash/raw/robust04/data/"
                "oakey" -> "/mnt/scratch/jfoley/robust04raw/"
                else -> error("Robust raw path needed.")
            })
        })

        val msg = CountingDebouncer(527_719)
        IreneIndexer.build {
            withPath(File("/media/jfoley/flash/robust.n2.irene2"))
            create()
        }.use { writer ->
            val docs_seq = tcs.docs()
            docs_seq.forEach { doc ->
                writer.doc {
                    setId(doc.name)
                    setEfficientTextField("body", doc.body, true)
                    maybeTextField("title", doc.title, true)
                    maybeTextField("date", doc.date, true)
                }
                msg.incr()?.let { upd ->
                    println("Indexing robust.irene2 $upd")
                }
            }
        }
    }
}


object SpeedBigramRobust {
    @JvmStatic fun main(args: Array<String>) {
        val fastParams = IndexParams().apply {
            withPath(File("/media/jfoley/flash/robust.n2.irene2"))
        }

        val dataset = DataPaths.Robust
        val slowIndex= dataset.getIreneIndex()
        val fastIndex = IreneIndex(fastParams)
        fastIndex.env.estimateStats = "min"
        fastIndex.env.indexedBigrams = true
        slowIndex.env.estimateStats = "min"

        val queries = dataset.desc_qs.mapValues { (_, qtext) -> slowIndex.tokenize(qtext) }
        val eval = getEvaluator("map")
        val qrels = dataset.qrels
        val info = NamedMeasures()

        queries.forEach { qid, qterms ->
            // fast-sdm
            val sdm = SequentialDependenceModel(qterms, stopwords= inqueryStop).map { q ->
                if (q is UnorderedWindowExpr) {
                    SmallerCountExpr(q.children)
                } else {
                    q
                }
            }

            val sdm2 = sdm.deepCopy()
            //sdm.applyEnvironment(slowIndex.env)
            //sdm2.applyEnvironment(fastIndex.env)

            //toSimpleString(sdm, System.out, "Q1>")
            //toSimpleString(sdm2, System.out, "Q2>")

            val (slowTime, slowResults) = timed { slowIndex.search(sdm, 1000) }
            val (fastTime, fastResults) = timed {
                fastIndex.search(sdm2, 1000)
            }

            info.push("fastTime", fastTime)
            info.push("slowTime", slowTime)

            val slowNamedResults = slowResults.toQueryResults(slowIndex, qid)
            val fastNamedResults = fastResults.toQueryResults(fastIndex, qid)

            info.push("slowAP", eval.evaluate(slowNamedResults, qrels[qid] ?: QueryJudgments(qid, emptyMap())))
            info.push("fastAP", eval.evaluate(fastNamedResults, qrels[qid] ?: QueryJudgments(qid, emptyMap())))

            println("$qid $qterms\n\t$info")
        }

        println(info)
    }
}

object ScoringMain {
    @JvmStatic
    fun main(args: Array<String>) {
        val dataset = DataPaths.Robust
        val qrels = dataset.getQueryJudgments()
        val queries = dataset.getTitleQueries()
        val evals = getEvaluators(listOf("ap", "ndcg"))
        val ms = NamedMeasures()

        dataset.getIndex().use { galago ->
            IreneIndex(IndexParams().apply {
                withPath(File("robust.irene2"))
            }).use { index ->
                val mu = index.getAverageDL("body")
                println(index.getStats(Term("body", "president")))

                queries.forEach { qid, qtext ->
                    //if (qid != "301") return@forEach
                    println("$qid $qtext")
                    val qterms = index.analyzer.tokenize("body", qtext)
                    val q = MeanExpr(qterms.map { DirQLExpr(TextExpr(it)) })

                    val gq = GExpr("combine").apply { addTerms(qterms) }

                    val top5 = galago.transformAndExecuteQuery(gq, pmake {
                        set("requested", 5)
                        set("annotate", true)
                        set("processingModel", "rankeddocument")
                    }).scoredDocuments
                    val topK = index.search(q, 1000)
                    val results = topK.toQueryResults(index)

                    //if (argp.get("printTopK", false)) {
                    (0 until Math.min(5, topK.totalHits.toInt())).forEach { i ->
                        val id = results[i]
                        val gd = top5[i]

                        if (i == 0 && id.name != gd.name) {
                            println(gd.annotation)
                            val missed = index.documentById(gd.name)!!
                            println("Missed document=$missed")
                            println(index.explain(q, missed))
                        }

                        println("${id.rank}\t${id.name}\t${id.score}")
                        println("${gd.rank}\t${gd.name}\t${gd.score}")
                    }
                    //}

                    val queryJudgments = qrels[qid]!!
                    evals.forEach { measure, evalfn ->
                        val score = try {
                            evalfn.evaluate(results, queryJudgments)
                        } catch (npe: NullPointerException) {
                            System.err.println("NULL in eval...")
                            -Double.MAX_VALUE
                        }
                        ms.push("$measure.irene2", score)
                    }

                    println(ms.means())
                }
            }
            println(ms.means())
        }
    }
}

object LuceneScorersMain {
    @JvmStatic
    fun main(args: Array<String>) {
        DataPaths.Robust.getIreneIndex().use { index ->
            val mu = index.env.defaultDirichletMu
            val stats = index.getStats("president")
            val bg = mu * stats.nonzeroCountProbability()
            index.reader.leaves().forEach { leaf ->
                println("Leaf: docBase=${leaf.docBase} ord=${leaf.ord} ordInParent=${leaf.ordInParent} N=${leaf.reader().numDocs()}")
                val lengths = leaf.reader().getNormValues(index.defaultField) ?: leaf.reader().getNumericDocValues("lengths:${index.defaultField}")

                val ss = StreamingStats()
                lengths.forEach { count ->
                    ss.push(count)
                }
                val worstCaseLength = ss.max

                for (c in listOf(0, 1, 10, worstCaseLength.toInt()/4)) {
                    val logDirProb = Math.log((c + bg) / (worstCaseLength + mu))
                    println("c=$c / l=$worstCaseLength => $logDirProb")
                }

                println(ss)
            }
        }
    }
}
