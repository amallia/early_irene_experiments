package edu.umass.cics.ciir.iltr.rf

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.iltr.loadRanklibViaJSoup
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.toQueryResults
import edu.umass.cics.ciir.learning.EnsembleNode
import edu.umass.cics.ciir.learning.FloatArrayVector
import edu.umass.cics.ciir.learning.TreeNode
import edu.umass.cics.ciir.learning.computeMeanVector
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.eval.EvalDoc
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.SimpleEvalDoc
import org.lemurproject.galago.utility.Parameters
import java.io.File
import kotlin.coroutines.experimental.buildSequence

/**
 *
 * @author jfoley.
 */
data class RanklibInput(val label: Double, val qid: String, val name: String, val features: FloatArrayVector, var prediction: Double = Double.NaN) : ScoredForHeap {
    val relevant: Boolean get() = label > 0
    override val score: Float get() = prediction.toFloat()
    lateinit var pvec: FloatArrayVector
    fun toEvalDoc(rank: Int) = SimpleEvalDoc(name, rank, prediction)
}

fun parseRanklibInput(line: String, dim: Int): RanklibInput {
    val (data, name) = line.maybeSplitAt('#')
    if (name == null) error("Must have named documents!")
    val tokens = data.trim().split(' ')
    val label = tokens[0].toDoubleOrNull() ?: error("First element must be label, found: ${tokens[0]}")
    if (!tokens[1].startsWith("qid:")) {
        error("Second token must be query identifier, found ${tokens[1]}")
    }
    val qid = tokens[1].substringAfter(':')
    val vector = FloatArrayVector(dim)
    for(i in (2 until tokens.size)) {
        val (fidStr, fvalStr) = tokens[i].splitAt(':') ?: error("Couldn't parse feature: ``${tokens[i]}'' ${tokens}")
        val fid = fidStr.toIntOrNull() ?: error("Couldn't parse feature id: $fidStr for token ${tokens[i]}")
        val fval = fvalStr.toDoubleOrNull() ?: error("Couldn't parse feature value: $fvalStr for ${tokens[i]}")
        vector[fid] = fval
    }
    return RanklibInput(label, qid, name, vector)
}

fun genRanklibInputs(input: File, dim: Int): Sequence<RanklibInput> = buildSequence {
    input.smartLines { lines ->
        for (line in lines) {
            yield(parseRanklibInput(line, dim))
        }
    }
}

fun genRanklibQueries(input: File, dim: Int): Sequence<List<RanklibInput>> = buildSequence {
    var qid: String? = null
    val items = ArrayList<RanklibInput>(1000)
    for (item in genRanklibInputs(input, dim)) {
        if (item.qid != qid && items.isNotEmpty()) {
            yield(items.toList())
            items.clear()
        }
        qid = item.qid
        items.add(item)
    }
    if (items.isNotEmpty()) {
        yield(items)
    }
}

const val metaFileSuffix = ".meta.json"
fun findMetaFile(ranklibFile: File): File {
    val dir = ranklibFile.parentFile
    val base = ranklibFile.name
    val direct = File(dir, base+ metaFileSuffix)
    if (direct.exists()) return direct
    if (base.endsWith(".gz")) {
        val unzMeta = File(dir, base.substringBeforeLast(".gz")+ metaFileSuffix)
        if (unzMeta.exists()) return unzMeta
    }
    error("Couldn't find feature information ``meta.json'' file for $ranklibFile")
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val data = File(argp.get("dir", "nyt-cite-ltr"))
    val dsName = argp.get("dataset", "gov2")
    val dataset = DataPaths.get(dsName)
    val qrels = dataset.qrels
    val map = getEvaluator("map")
    val p10 = getEvaluator("p10")
    val modelName = when(dsName) {
        "trec-core" -> "lm.t200.l32"
        "gov2" -> "mq07.lambdaMart.l10.kcv10.tvs90.gz"
        else -> error("Model name for $dataset")
    }

    val ranklibInputName = "${dsName}.features.ranklib.gz"
    val meta = Parameters.parseFile(findMetaFile(File(data, ranklibInputName)))

    println(meta)
    // Just add a buffer since Ranklib features are off-by-one.
    val dim = meta.size + 1
    println("Dim: $dim")

    val model: TreeNode = loadRanklibViaJSoup(File(data, modelName).absolutePath)
    val numTrees = (model as EnsembleNode).guesses.size
    println("Num Trees: $numTrees")

    val queries = HashMap<String, List<RanklibInput>>(250)
    genRanklibQueries(File(data, ranklibInputName), dim).forEach { docs ->
        val qid = docs[0].qid
        println(" -> $qid ${docs.size} ${docs.count { it.relevant }}")
        // Predict via model.
        docs.forEach {
            val predv = model.predictionVector(it.features.data)
            it.prediction = predv.mean()
            it.pvec = FloatArrayVector(predv.map { it.toFloat() }.toFloatArray())
            //it.prediction = model.score(it.features.data)
            //it.prediction = it.features[162]
            //it.prediction = it.features[79] // sdm-stop for gov2
        }
        // sort/rank by the model
        queries[qid] = docs.sortedByDescending { it.prediction }
    }

    val user = queries.mapValues { (_, docs) -> InteractionFeedback(docs, 10) }

    val fbCounts = HashMap<Int,Int>()
    user.forEach { qid, fb ->
        fbCounts.incr(fb.correct.size, 1)
    }

    val index = DataPaths.Gov2.getIreneIndex()
    val fbField = argp.get("fbField", index.defaultField)
    val fbTerms = argp.get("fbTerms", 100)

    val measures = NamedMeasures()
    user.forEach { qid, fb ->
        val judgments = qrels[qid] ?: QueryJudgments(qid, emptyMap())
        val ranking = fb.docs.mapIndexed { i, rli -> rli.toEvalDoc(i+1) }
        println("$qid\t\t$measures")

        val fbRaw = computeMeanVector(fb.correct.map { it.features })
        val fbLTR = computeMeanVector(fb.correct.map { it.pvec })
        if (fbRaw != null) {
            fbLTR!!
            val fbRaw_ranking = fb.rankRemaining { fbRaw.dotp(it.features) }
            val fbLTR_ranking = fb.rankRemaining { fbLTR.dotp(it.pvec) }
            measures.ppush(qid,"FB-RAW-AP[10..]", map.evaluate(QueryResults(qid, fbRaw_ranking), judgments))
            measures.ppush(qid,"FB-LTR-AP[10..]", map.evaluate(QueryResults(qid, fbLTR_ranking), judgments))

            val whitelist = WhitelistMatchExpr(fb.remaining.mapTo(HashSet()) { it.name })
            val correctDocTerms = fb.correct.mapNotNull {index.documentById(it.name)}.map { index.terms(it, fbField) }

            val fbModel = HashMap<String, Double>()
            correctDocTerms.forEach { terms ->
                terms.forEach { term ->
                    if (!inqueryStop.contains(term)) {
                        fbModel.incr(term, 1.0)
                    }
                }
            }
            val bestFbTerms = ScoringHeap<ScoredWord>(fbTerms)
            fbModel.forEach { t, s -> bestFbTerms.offer(s.toFloat(), { ScoredWord(s.toFloat(), t) }) }

            val expQ = SumExpr(bestFbTerms.sorted
                    .associate { Pair(it.word, it.score.toDouble()) }
                    .normalize()
                    .map { DirQLExpr(TextExpr(it.key)).weighted(it.value) })

            val rerankQ = RequireExpr(whitelist, expQ)
            println("Submit: expQ=${expQ.findTextNodes().map { it.text }}")
            val tfRocchioTopDocs = index.search(rerankQ, whitelist.docNames?.size ?: 0)
            val tfRocchioPosResults = tfRocchioTopDocs.toQueryResults(index, qid)

            // traditional feedback
            measures.ppush(qid, "TF-FB[10..]", map.evaluate(tfRocchioPosResults, judgments))

            // regular!
            measures.ppush(qid,"AP[0..]", map.evaluate(QueryResults(qid, ranking), judgments))
            measures.ppush(qid,"AP[10..]", map.evaluate(QueryResults(qid, ranking.drop(10)), judgments))
            measures.ppush(qid,"AP[20..]", map.evaluate(QueryResults(qid, ranking.drop(20)), judgments));
            measures.ppush(qid,"P10", p10.evaluate(QueryResults(qid, ranking), judgments));
        }
    }

    println(measures)
    println(fbCounts)
    index.close()
}

data class ScoredRanklibInput(override val score: Float, val original: RanklibInput) : ScoredForHeap

data class InteractionFeedback(val correct: List<RanklibInput>, val incorrect: List<RanklibInput>, val remaining: List<RanklibInput>) {
    constructor(ranking: List<RanklibInput>, depth: Int) : this(ranking.take(depth).filter { it.relevant }, ranking.take(depth).filterNot { it.relevant }, ranking.drop(depth))
    val docs: List<RanklibInput> get() = (correct + incorrect + remaining).sortedByDescending { it.prediction }
    fun rankRemaining(scorer: (RanklibInput)->Double): List<EvalDoc> {
        val output = ScoringHeap<ScoredRanklibInput>(remaining.size)
        remaining.forEach { rdoc ->
            output.offer(ScoredRanklibInput(scorer(rdoc).toFloat(), rdoc))
        }
        return output.sorted.mapIndexed { i, sri -> SimpleEvalDoc(sri.original.name, i+1, sri.score.toDouble()) }
    }
}