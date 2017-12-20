package edu.umass.cics.ciir.iltr.rf

import edu.umass.cics.ciir.chai.ScoredForHeap
import edu.umass.cics.ciir.chai.maybeSplitAt
import edu.umass.cics.ciir.chai.smartLines
import edu.umass.cics.ciir.chai.splitAt
import edu.umass.cics.ciir.iltr.loadRanklibViaJSoup
import edu.umass.cics.ciir.learning.EnsembleNode
import edu.umass.cics.ciir.learning.FloatArrayVector
import edu.umass.cics.ciir.learning.TreeNode
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluator
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
    val meta = Parameters.parseFile(File(data, "${ranklibInputName}.meta.json"))

    println(meta)
    // Just add a buffer since Ranklib features are off-by-one.
    val dim = meta.size + 1
    println("Dim: $dim")

    val model: TreeNode = loadRanklibViaJSoup(File(data, modelName).absolutePath)
    println("Num Trees: ${(model as EnsembleNode).guesses.size}")

    val queries = HashMap<String, List<RanklibInput>>(250)
    genRanklibQueries(File(data, ranklibInputName), dim).forEach { docs ->
        val qid = docs[0].qid
        println(" -> $qid ${docs.size} ${docs.count { it.relevant }}")
        // Predict via model.
        docs.forEach {
            it.prediction = model.score(it.features.data)
            //it.prediction = it.features[162]
            //it.prediction = it.features[79] // sdm-stop for gov2
        }
        // sort/rank by the model
        queries[qid] = docs.sortedByDescending { it.prediction }
    }

    val measures = NamedMeasures()
    queries.forEach { qid, docs ->
        val judgments = qrels[qid] ?: QueryJudgments(qid, emptyMap())
        val ranking = docs.mapIndexed { i, rli -> rli.toEvalDoc(i+1) }
        measures.push("AP[0..]", map.evaluate(QueryResults(qid, ranking), judgments))
        measures.push("AP[10..]", map.evaluate(QueryResults(qid, ranking.drop(10)), judgments))
        measures.push("AP[20..]", map.evaluate(QueryResults(qid, ranking.drop(20)), judgments));
        println("$qid\t\t$measures")
    }

    println(measures)
}