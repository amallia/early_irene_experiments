package edu.umass.cics.ciir.searchie

import edu.umass.cics.ciir.irene.utils.CountingDebouncer
import edu.umass.cics.ciir.irene.utils.smartLines
import edu.umass.cics.ciir.irene.utils.smartPrint
import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndex
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.galago.*
import edu.umass.cics.ciir.irene.lang.QueryLikelihood
import edu.umass.cics.ciir.irene.lang.SequentialDependenceModel
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.Parameters
import java.io.File

/**
 * @author jfoley
 */
val FIGER_DIR = File("/mnt/scratch/jfoley/FIGER")

data class FigerMention(val start: Int, val end: Int, val labels: List<String>)
data class FigerExample(val id: Int, val tokens: List<String>, val mentions: List<FigerMention>) {
    val text: String by lazy { tokens.joinToString(separator = " ")}
}

fun parseExample(p: Parameters) =
    FigerExample(
            p.getInt("senid"),
            p.getAsList("tokens", String::class.java),
            p.getAsList("mentions", Parameters::class.java).map { parseMention(it) })
fun parseMention(p: Parameters) = FigerMention(p.getInt("start"), p.getInt("end"), p.getAsList("labels", String::class.java))

val BBN_DIR = File(FIGER_DIR, "BBN")
val BBN_NUM_EX = 32739

val WIKI_DIR = File(FIGER_DIR, "Wiki")
val WIKI_NUM_EX = 1505765
val WIKI_NC = 128

val dir = WIKI_DIR
val count = WIKI_NUM_EX.toLong()
val numClasses = WIKI_NC.toLong()
val ValidationSize = 200

fun main(args: Array<String>) {
    val exact = HashMap<String, MutableList<String>>()
    val neighbors = HashMap<String, MutableList<String>>()
    val msg = CountingDebouncer(total = count + numClasses)
    File(dir, "train.json.gz").smartLines { lines ->
        lines.drop(ValidationSize).forEach { line ->
            val ex = parseExample(Parameters.parseString(line))

            for (mention in ex.mentions) {
                val tokens = ex.tokens.subList(mention.start, mention.end).joinToString(separator = " ")
                for (label in mention.labels) {
                    exact.computeIfAbsent(label, {ArrayList()}).add(tokens)
                    neighbors.computeIfAbsent(label, {ArrayList()}).add(ex.text)
                }
            }

            msg.incr()?.let { upd ->
                println("hash: ${exact.size} $upd")
            }
        }
    }

    File(dir, "memorize_doc.jsonl.gz").smartPrint { out ->
        neighbors.forEach { label, terms ->
            out.println(pmake {
                set("id", label)
                set("body", terms)
                set("exact", exact[label])
            })
            msg.incr()?.let { upd ->
                println("writing: $upd")
            }
        }
    }
}

object BuildIndex {
    @JvmStatic fun main(args: Array<String>) {
        IreneIndexer.build {
            withPath(File(dir, "memorize.irene2"))
            create()
        }.use { indexer ->
            val msg = CountingDebouncer(total = numClasses)
            File(dir, "memorize_doc.jsonl.gz").smartLines { lines ->
                lines.forEach { line ->
                    val p = Parameters.parseString(line)
                    indexer.doc {
                        setId(p.getStr("id"))
                        setTextField("body",
                                p.getAsList("body", String::class.java)
                                        .joinToString(separator = "\n"), stored = false)
                        setTextField("exact",
                                p.getAsList("exact", String::class.java)
                                        .joinToString(separator = "\n"), stored = false)
                    }
                    msg.incr()?.let { upd ->
                        println(upd)
                    }
                }
            }
        }
    }
}

object TestWithIndex {
    @JvmStatic fun main(args: Array<String>) {
        val measures = getEvaluators("ap", "recip_rank", "r-prec")
        val valiQueries =
                File(dir, "train.json.gz").smartLines { lines ->
                    lines.take(ValidationSize).map { line -> parseExample(Parameters.parseString(line)) }.toList()
                }
        val results = NamedMeasures()
        println("testQueries.size=${valiQueries.size}")

        val vali_qrels = HashMap<String, QueryJudgments>()
        val vali_queries = HashMap<String, FigerExample>()

        IreneIndex(IndexParams().apply {
            withPath(File(dir, "memorize.irene2"))
        }).use { index ->
            valiQueries.forEachIndexed { i: Int, ex ->
                val qid = "v$i"
                val labels = ex.mentions.flatMapTo(HashSet()) { it.labels }
                val qtext = ex.text
                val qrels = QueryJudgments(qid, labels.associate { Pair(it, 1) })
                val terms = index.tokenize(qtext)

                vali_qrels[qid] = qrels
                vali_queries[qid] = ex

                val sdm = index.search(SequentialDependenceModel(terms, stopwords= inqueryStop), 1000).toQueryResults(index, qid)
                val ql = index.search(QueryLikelihood(terms), 1000).toQueryResults(index, qid)
                val qlE = index.search(QueryLikelihood(terms, field="exact", statsField = "body"), 1000).toQueryResults(index, qid)
                measures.forEach { (m,fn) ->
                    results.push("SDM-$m", fn.evaluate(sdm, qrels))
                    results.push("QL.body-$m", fn.evaluate(ql, qrels))
                    results.push("QL.exact-$m", fn.evaluate(qlE, qrels))
                }
                println(results)
            }
        }

        println("---")
        println(results)
    }
}

// exp 1:
// QL-ap=0.161	QL-r-prec=0.103	QL-recip_rank=0.215
// SDM-ap=0.159	SDM-r-prec=0.095	SDM-recip_rank=0.197
//
// stop-sdm:
// QL.body-ap=0.161	QL.body-r-prec=0.103	QL.body-recip_rank=0.215
// QL.exact-ap=0.025	QL.exact-r-prec=0.007	QL.exact-recip_rank=0.029
// SDM-ap=0.146	SDM-r-prec=0.084	SDM-recip_rank=0.177