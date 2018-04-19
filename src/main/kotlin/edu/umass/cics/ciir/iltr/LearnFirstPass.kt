package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.sprf.example.selectSingleChild
import edu.umass.cics.ciir.irene.galago.getEvaluator
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.galago.incr
import edu.umass.cics.ciir.irene.galago.pmake
import edu.umass.cics.ciir.irene.utils.*
import edu.umass.cics.ciir.learning.*
import edu.umass.cics.ciir.sprf.*
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.jsoup.parser.Parser
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.retrieval.ScoredDocument
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.lists.Ranked
import java.io.File

/**
 *
 * @author jfoley.
 */

val fixedFeatures = listOf("tf", "length", "prob", "binary", "avgdl", "cbg", "bbg", "cf", "df", "cl", "dc")
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsn = argp.get("dataset", "robust")
    val dataset = DataPaths.get(dsn)
    val queries = dataset.getTitleQueries()
    val qrels = dataset.qrels

    val nameToId = HashMap<String, Int>()

    val docIdPool = HashSet<Int>()
    dataset.getIreneIndex().use { index ->
        index.env.estimateStats = "min"

        val sample = ReservoirSampler<Int>(2000)
        (0 until index.totalDocuments).forEach { docId -> sample.process(docId) }
        docIdPool.addAll(sample)

        File("$dsn.firstpass.jsonl.gz").smartPrint { writer ->
            queries.forEach { qid, qtext ->
                val qterms = index.tokenize(qtext)
                //val sdm = SequentialDependenceModel(qterms, stopwords = inqueryStop)
                //val top200 = index.search(sdm, 200)
                //docIdPool.addAll(top200.scoreDocs.map { it.doc })

                val judgments = qrels[qid]
                val (nonrel, rel) = judgments.entries.map { Pair(it.key!!, it.value ?: 0) }.partition { it.second == 0 }

                val pool = HashSet<Int>(docIdPool)

                println("$qid. ${docIdPool.size} ${rel.size} ${nonrel.size} $qtext")
                qrels[qid].documentSet.forEach { judged ->
                    val num = nameToId.computeIfAbsent(judged, { index.documentById(it) ?: -1 })
                    if (num != -1) {
                        pool.add(num)
                    }
                }

                for (docid in pool) {
                    val fields = index.docAsParameters(docid) ?: continue
                    val id = fields.getStr(index.idFieldName)
                    val label = qrels[qid][id]
                    val instance = pmake {
                        set("name", id)
                        set("label", label)
                        set("qid", qid)
                    }
                    val bag = HashMap<String, Int>()
                    val docTerms = index.tokenize(index.getField(docid, index.defaultField)?.stringValue() ?: "")
                    docTerms.forEach { bag.incr(it, 1) }

                    val perTermFeatures = qterms.map { term ->
                        val countStats = index.getStats(term)

                        pmake {
                            set("tf", bag[term] ?: 0)
                            set("length", docTerms.size)
                            set("binary", bag.containsKey(term))
                            set("prob", safeDiv(bag[term] ?: 0, docTerms.size))

                            set("avgdl", countStats.avgDL())
                            set("cbg", countStats.nonzeroCountProbability())
                            set("bbg", countStats.binaryProbability())

                            set("cf", countStats.cf)
                            set("df", countStats.df)
                            set("cl", countStats.cl)
                            set("dc", countStats.dc)
                        }
                    }

                    instance.set("terms", perTermFeatures)
                    writer.println(instance)
                }
            }
        }
    }
}

object PrepareTermRanklibInput {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dsn = argp.get("dataset", "robust")
        File("$dsn.firstpass.ranklib.gz").smartPrint { output ->
            File("$dsn.firstpass.jsonl.gz").smartDoLines { line ->
                val json = Parameters.parseString(line)
                val label = json.getInt("label")
                val qid = json.getStr("qid")
                val name = json.getStr("name")

                val rlstrs = json.children("terms").mapIndexed { i, ftrs ->
                    val instName = "$name.t$i"
                    fixedFeatures.mapIndexed { fid, fname ->
                        val raw = ftrs[fname]
                        val score = when(raw) {
                            null -> 0.0
                            is Boolean -> if (raw) { 1.0 } else { -1.0 }
                            is Number -> raw.toDouble()
                            else -> error("Raw=$raw")
                        }

                        Pair(fid+1, score)
                    }.joinToString(
                                    separator = " ",
                                    prefix = "$label qid:$qid ",
                                    postfix = " #$instName"
                            ) { (fid, fval) -> "$fid:$fval" }
                }

                val keep = if (label != 0) {
                    rlstrs
                } else {
                    rlstrs.sample(1)
                }

                for (rl in keep) {
                    output.println(rl)
                }
            }
        }
    }
}

fun Element.childWithTagOrDie(tag: String) = this.children().find { it.tagName() == tag } ?: error("Missing tag=$tag $this")

fun parseElement(el: Element): TreeNode {
    when(el.tagName()) {
        "ensemble" -> {
            val trees = el.children().filter { it.tagName() == "tree" }.map { parseElement(it) }
            return EnsembleNode(trees)
        }
        "tree" -> {
            val weight = el.attr("weight").toDouble()
            val inner = el.children().find { it.tagName() == "split" } ?: error("No split in tree...")
            val tree = parseElement(inner)
            tree.weight = weight
            return tree
        }
        "split" -> {
            val output = el.children().find { it.tagName() == "output" }
            if (output != null) {
                return LeafResponse(output.text().toDouble())
            }

            val splitChildren = el.children().filter { it.tagName() == "split" }
            val feature = el.childWithTagOrDie("feature").text().toInt()
            val threshold = el.childWithTagOrDie("threshold").text().toDouble()

            val left = splitChildren.find { it.attr("pos") == "left" } ?: error("No left.")
            val right = splitChildren.find { it.attr("pos") == "right" } ?: error("No right.")

            return FeatureSplit(feature, threshold, parseElement(left), parseElement(right))
        }
        else -> error("Unhandled-tag: $el")
    }
}

fun loadRanklibViaJSoup(input: String): TreeNode {
    return File(input).smartLines { lines ->
        val ensembleHTML = Jsoup.parse(lines
                .dropWhile { it.startsWith("##") }
                .joinToString(separator = "\n")
                , "", Parser.xmlParser())
        parseElement(ensembleHTML.selectSingleChild("ensemble") ?: error("Couldn't find an <ensemble> tag in $input"))
    }

}

object EvalRanklibForest {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dsn = argp.get("dataset", "robust")
        val dataset = DataPaths.get(dsn)
        val model = loadRanklibViaJSoup("term_forest")
        val qrels = dataset.qrels
        val measure = getEvaluator("ap")

        val data = HashMap<String, MutableList<ScoredDocument>>()
        val ql = HashMap<String, MutableList<ScoredDocument>>()

        File("$dsn.firstpass.jsonl.gz").smartDoLines { line ->
            val json = Parameters.parseString(line)
            val label = json.getInt("label")
            val qid = json.getStr("qid")
            val name = json.getStr("name")

            // Mean (term) RF: 0.198 when per-term was .230
            // min is much worse.
            // QL: 0.286
            val pred = json.children("terms").meanByDouble { ftrs ->
                val vec = FloatArray(fixedFeatures.size+1)
                fixedFeatures.forEachIndexed { fid, fname ->
                    val raw = ftrs[fname]
                    val score = when (raw) {
                        null -> 0.0
                        is Boolean -> if (raw) {
                            1.0
                        } else {
                            -1.0
                        }
                        is Number -> raw.toDouble()
                        else -> error("Raw=$raw")
                    }
                    vec[fid+1] = score.toFloat()
                }

                model.score(vec)
            }

            val mu = 1500.0
            val qlScore = json.children("terms").meanByDouble { ftrs ->
                val tf = ftrs.getDouble("tf")
                val len = ftrs.getDouble("length")
                val cbg = ftrs.getDouble("cbg")
                val prob = (tf + mu * cbg) / (len + mu)
                Math.log(prob)
            }

            data.push(qid, ScoredDocument(name, -1, pred))
            ql.push(qid, ScoredDocument(name, -1, qlScore))
        }

        val rfAP = data.entries.toList().meanByDouble { (qid, docs) ->
            Ranked.setRanksByScore(docs)
            measure.evaluate(QueryResults(docs), qrels[qid])
        }
        val qlAP = ql.entries.toList().meanByDouble { (qid, docs) ->
            Ranked.setRanksByScore(docs)
            measure.evaluate(QueryResults(docs), qrels[qid])
        }
        println("RF: $rfAP")
        println("QL: $qlAP")
    }
}

fun <E> List<E>.minByDouble(fn: (E)->Double): Double {
    var minimum = Double.MAX_VALUE
    for (item in this) {
        val cur = fn(item)
        if (cur < minimum) {
            minimum = cur
        }
    }
    return minimum
}
