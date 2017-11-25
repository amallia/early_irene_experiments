package edu.umass.cics.ciir.learning

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.iltr.pagerank.SpacesRegex
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.getEvaluator
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.core.eval.metric.QueryEvaluator
import org.lemurproject.galago.core.retrieval.ScoredDocument
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.lists.Ranked
import java.io.File
import java.util.*
import java.util.concurrent.ThreadLocalRandom

/**
 *
 * @author jfoley.
 */
sealed class TreeNode {
    var weight: Double = 1.0
    abstract fun score(features: FloatArray): Double
}
data class EnsembleNode(val guesses: List<TreeNode>): TreeNode() {
    override fun score(features: FloatArray): Double = weight * guesses.meanByDouble { it.score(features) }
}
data class FeatureSplit(val fid: Int, val point: Double, val lhs: TreeNode, val rhs: TreeNode): TreeNode() {
    override fun score(features: FloatArray): Double = if (features[fid] < point) {
        weight * lhs.score(features)
    } else {
        weight * rhs.score(features)
    }
}

data class LeafResponse(val probability: Double) : TreeNode() {
    override fun score(features: FloatArray): Double = weight * probability
}

data class QDoc(val label: Float, val qid: String, val features: FloatArray, val name: String) {
    private val identity: String = "$qid:$name"
    private val idHash = identity.hashCode()
    override fun hashCode(): Int = idHash
    override fun equals(other: Any?): Boolean {
        if (other is QDoc) {
            return idHash == other.idHash && identity == other.identity
        }
        return false
    }
}

fun String.splitAt(c: Char): Pair<String, String>? {
    val pos = this.indexOf(c)
    if (pos < 0) return null
    return Pair(this.substring(0, pos), this.substring(pos+1))
}

data class CVSplit(val id: Int, val trainIds: Set<String>, val valiIds: Set<String>, val testIds: Set<String>) {

    fun evaluate(dataset: Map<String, List<QDoc>>, measure: QueryEvaluator, qrels: QuerySetJudgments, tree: TreeNode): Double {
        return dataset.toList().meanByDouble { (qid, inputList) ->
            val ranked = inputList.mapTo(ArrayList()) {
                val pred = tree.score(it.features)
                ScoredDocument(it.name, -1, pred)
            }
            Ranked.setRanksByScore(ranked)
            measure.evaluate(QueryResults(ranked), qrels[qid] ?: QueryJudgments(qid, emptyMap()))
        }
    }
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dataset = argp.get("dataset", "gov2")
    val input = argp.get("input", "l2rf/$dataset.features.ranklib")
    val featureNames = Parameters.parseFile(argp.get("meta", "$input.meta.json"))
    val numFeatures = argp.get("numFeatures",
            featureNames.size)
    val numTrees = argp.get("numTrees", 100)
    val sampleRate = argp.get("srate", 0.25)
    val featureSampleRate = argp.get("frate", 0.05)
    val kSplits = argp.get("kcv", 5)
    val querySet = DataPaths.get(dataset)
    val queries = querySet.title_qs
    val qrels = querySet.qrels
    val measure = getEvaluator("ap")

    val splitQueries = HashMap<Int, MutableList<String>>()
    (queries.keys + qrels.keys).toSet().sorted().forEachIndexed { i, qid ->
        splitQueries.push(i%kSplits, qid)
    }

    val splits = (0 until kSplits).map { index ->
        val testId = index
        val valiId = (index+1) % kSplits
        val trainIds = (0 until kSplits).filter { it != testId  && it != valiId }.toSet()
        val testQs = splitQueries[testId]!!.toSet()
        val valiQs = splitQueries[valiId]!!.toSet()
        val trainQs = trainIds.flatMap { splitQueries[it]!! }.toSet()

        println(trainQs.size)
        println(valiQs.size)
        println(testQs.size)

        CVSplit(index, trainQs, valiQs, testQs)
    }

    println("numFeatures: $numFeatures numSplits: $kSplits")

    val byQuery = HashMap<String, MutableList<QDoc>>()
    File(input).smartDoLines(true, total=150_000L) { line ->
        val (ftrs, doc) = line.splitAt('#') ?: error("Can't find doc!")
        val row = ftrs.trim().split(SpacesRegex)
        val label = row[0].toFloatOrNull() ?: error("Can't parse label as float.")
        val qid = row[1].substringAfter("qid:")
        if (qid.isBlank()) {
             error("Can't find qid: $row")
        }

        val fvec = FloatArray(numFeatures)
        (3 until row.size).forEach { i ->
            val (fid, fval) = row[i].splitAt(':') ?: error("Feature must have : split ${row[i]}.")
            fvec[fid.toInt()-1] = fval.toFloat()
        }

        byQuery.push(qid, QDoc(label, qid, fvec, doc))
    }

    splits.forEach { split ->
        if (split.id != 0) return

        val trainInsts = split.trainIds.flatMap { byQuery[it]!! }
        val trainFStats = (0 until numFeatures).map { StreamingStats() }
        trainInsts.forEach { doc ->
            doc.features.forEachIndexed { fid, fval -> trainFStats[fid].push(fval.toDouble()) }
        }

        val kFeatures = argp.get("numFeatures", (featureSampleRate * numFeatures).toInt())
        val kSamples = argp.get("numSamples", (sampleRate * trainInsts.size).toInt())
        if (kSamples <= 1) {
            error("Cannot function with few samples. Only selects $kSamples in practice.")
        }

        val trainSet = trainInsts.groupBy { it.qid }
        val testSet = split.testIds.flatMap { byQuery[it]!! }.groupBy { it.qid }
        val valiSet = split.valiIds.flatMap { byQuery[it]!! }.groupBy { it.qid }

        val outputTrees = ArrayList<TreeNode>()

        while(outputTrees.size < numTrees) {
            val f_sample = (0 until numFeatures).sample(kFeatures).toList()
            val x_sample = trainInsts.sample(kSamples).toList()
            //println(f_sample)

            val outOfBag = HashSet(trainInsts).apply { removeAll(x_sample) }.groupBy { it.qid }

            if (x_sample.none { it.label > 0 }) {
                println("Note: bad sample, no positive labels in sample.")
                continue
            }

            val tree = trainTree(trainFStats, f_sample, x_sample)
            if (tree == null) {
                println("Could not learn a tree from this sample...")
                continue
            }
            //println("Learned tree $tree")

            //val trainAP = split.evaluate(trainSet, measure, qrels, tree)
            val oobAP = split.evaluate(outOfBag, measure, qrels, tree)
            //val valiAP = split.evaluate(valiSet, measure, qrels, tree)
            //println("\ttrain-AP: $trainAP, oob-AP: $oobAP, vali-AP: $valiAP")

            tree.weight = oobAP
            outputTrees.add(tree)

            if (outputTrees.size > 1) {
                val ensemble = EnsembleNode(outputTrees)
                val trainAP = split.evaluate(trainSet, measure, qrels, ensemble)
                val valiAP = split.evaluate(valiSet, measure, qrels, ensemble)
                val testAP = split.evaluate(testSet, measure, qrels, ensemble)
                println("ENSEMBLE[]%d train-AP: %1.3f, vali-AP: %1.3f, test-AP: %1.3f".format(outputTrees.size, trainAP, valiAP, testAP))
                //println("ENSEMBLE[]${outputTrees.size} train-AP: $trainAP, vali-AP: $valiAP, test-AP: $testAP")
            }
        }

        val ensemble = EnsembleNode(outputTrees)
        val testAP = split.evaluate(testSet, measure, qrels, ensemble)

        println("Split: ${split.id} Test-AP: ${"%1.3f".format(testAP)}")
    }
}

class InstanceSet {
    val instances = ArrayList<QDoc>()
    val labelStats = StreamingStats()
    val size: Int get() = instances.size
    val output: Double get() = labelStats.mean

    fun push(x: QDoc) {
        labelStats.push(x.label.toDouble())
        instances.add(x)
    }
}

data class FeatureSplitCandidate(val fid: Int, val split: Double) {
    val lhs = InstanceSet()
    val rhs = InstanceSet()
    fun consider(instances: Collection<QDoc>) {
        instances.forEach { inst ->
            if (inst.features[fid] < split) {
                lhs.push(inst)
            } else {
                rhs.push(inst)
            }
        }
    }
    // Actually splits the data:
    val isGoodSplit: Boolean get() = lhs.size != 0 && rhs.size != 0
    // Estimate the usefulness as the difference in label means between the splits.
    val usefulness: Double get() = Math.abs(rhs.labelStats.mean - lhs.labelStats.mean)
}

fun trainTree(fStats: List<StreamingStats>, features: Collection<Int>, instances: Collection<QDoc>, score: Double? = null): TreeNode? {
    val splits = features.mapNotNull { fid ->
        val stats = fStats[fid]
        if (stats.min == stats.max) {
            return@mapNotNull null
        }
        FeatureSplitCandidate(fid,
                ThreadLocalRandom.current().nextDouble(stats.min, stats.max)).apply {
            consider(instances)
        }
    }.filter { it.isGoodSplit }
    if (splits.isEmpty()) return null

    // TODO extract to strategy argument somehow.
    val bestFeature = splits.maxBy { it.usefulness }

    // Best feature found and there's a point in recursion:
    if (bestFeature != null && features.size > 1) {
        val splitF = bestFeature.fid
        val splitPoint = bestFeature.split
        val remainingFeatures = HashSet<Int>(features).apply { remove(splitF) }
        val lhs = bestFeature.lhs
        val rhs = bestFeature.rhs

        val lhsCond = trainTree(fStats, remainingFeatures, lhs.instances, lhs.output) ?: LeafResponse(lhs.output)
        val rhsCond = trainTree(fStats, remainingFeatures, rhs.instances, rhs.output) ?: LeafResponse(rhs.output)
        return FeatureSplit(splitF, splitPoint, lhsCond, rhsCond)
    }
    if (score == null) return null
    return LeafResponse(score)
}
