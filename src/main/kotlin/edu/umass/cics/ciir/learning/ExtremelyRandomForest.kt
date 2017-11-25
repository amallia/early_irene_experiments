package edu.umass.cics.ciir.learning

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.iltr.pagerank.SpacesRegex
import edu.umass.cics.ciir.irene.example.safeDiv
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


val mapper = ObjectMapper().registerModule(KotlinModule())

/**
 *
 * @author jfoley.
 */
sealed class TreeNode {
    var weight: Double = 1.0
    abstract fun score(features: FloatArray): Double
    abstract fun depth(): Int
}
data class EnsembleNode(val guesses: List<TreeNode>): TreeNode() {
    override fun depth(): Int = guesses.map { it.depth() }.max() ?: 0
    override fun score(features: FloatArray): Double = weight * guesses.meanByDouble { it.score(features) }
}
data class FeatureSplit(val fid: Int, val point: Double, val lhs: TreeNode, val rhs: TreeNode): TreeNode() {
    override fun score(features: FloatArray): Double = if (features[fid] < point) {
        weight * lhs.score(features)
    } else {
        weight * rhs.score(features)
    }
    override fun depth(): Int = 1 + maxOf(lhs.depth(), rhs.depth())
}

data class LeafResponse(val probability: Double) : TreeNode() {
    override fun score(features: FloatArray): Double = weight * probability
    override fun depth(): Int = 1
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
    val perceptronLabel: Int = if (label > 0) { 1 } else { -1 }
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

data class RLDataset(val byQuery: Map<String, MutableList<QDoc>>)

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

    val inputF = File(input)
    val cborInput = File("$input.cbor.gz")
    if (cborInput.exists()) {
        cborInput.bufferedReader().use { br ->
            byQuery.putAll(mapper.readValue<RLDataset>(br).byQuery)
        }
    } else {
        inputF.smartDoLines(true, total = 150_000L) { line ->
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
                fvec[fid.toInt() - 1] = fval.toFloat()
            }

            byQuery.push(qid, QDoc(label, qid, fvec, doc))
        }

        cborInput.bufferedWriter().use { ow ->
            mapper.writeValue(ow, RLDataset(byQuery))
        }
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

            val tree = trainTree(TreeLearningParams(trainFStats), f_sample, x_sample)
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
                println("ENSEMBLE[]%d.d=%d train-AP: %1.3f, vali-AP: %1.3f, test-AP: %1.3f".format(outputTrees.size, outputTrees.last().depth(), trainAP, valiAP, testAP))
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
    val perceptronLabel: Int = if (output >= 0) { -1 } else { 1 }

    fun push(x: QDoc) {
        val label = x.perceptronLabel
        labelStats.push(label)
        instances.add(x)
    }

    fun giniImpurity(): Double {
        val actualLabel = perceptronLabel
        val count = instances.size
        val mistakeCount = instances.count { it.perceptronLabel != actualLabel }
        val correctCount = count - mistakeCount

        val p_correct = safeDiv(correctCount, count)
        val p_mistake = safeDiv(mistakeCount, count)

        // this is going to be symmetric (choose correct, predict mistake) and (choose mistake, predict correct).
        val p_choose_and_wrong = p_correct * p_mistake
        return p_choose_and_wrong + p_choose_and_wrong
    }
    fun plogp(p: Double): Double {
        if (p == 0.0) return 0.0
        return p * Math.log(p)
    }
    fun entropy(): Double {
        val actualLabel = perceptronLabel
        val count = instances.size
        val mistakeCount = instances.count { it.perceptronLabel != actualLabel }
        val correctCount = count - mistakeCount

        val p_correct = safeDiv(correctCount, count)
        val p_mistake = safeDiv(mistakeCount, count)
        return -plogp(p_correct) - plogp(p_mistake)
    }
}

data class FeatureSplitCandidate(val fid: Int, val split: Double) {
    val lhs = InstanceSet()
    val rhs = InstanceSet()
    fun considerSorted(instances: List<QDoc>) {
        var splitPoint = 0
        var index = 0
        for (inst in instances) {
            val here = index++
            if (inst.features[fid] >= split) {
                splitPoint = here
                break
            }
        }

        (0 until splitPoint).forEach { lhs.push(instances[it]) }
        (splitPoint until instances.size).forEach { rhs.push(instances[it]) }
    }
    fun leftLeaf(): LeafResponse = LeafResponse(lhs.output)
    fun rightLeaf(): LeafResponse = LeafResponse(rhs.output)
}

interface ImportanceStrategy {
    fun importance(fsc: FeatureSplitCandidate): Double
}
class DifferenceInLabelMeans : ImportanceStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        return Math.abs(fsc.rhs.labelStats.mean - fsc.lhs.labelStats.mean)
    }
}
// The best split is the one where the standard deviation of labels goes very low.
// Ideally in both splits, here just in one is enough (we'll split the other again.
class MinLabelStdDeviation : ImportanceStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        return -minOf(fsc.rhs.labelStats.standardDeviation, fsc.lhs.labelStats.standardDeviation)
    }
}
class MultLabelStdDeviation : ImportanceStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        return -fsc.rhs.labelStats.standardDeviation * fsc.lhs.labelStats.standardDeviation
    }
}
// Typically want to minimize impurity, so negative.
class BinaryGiniImpurity : ImportanceStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        val lhs_size = fsc.lhs.size.toDouble()
        val rhs_size = fsc.rhs.size.toDouble()
        val size = lhs_size + rhs_size
        return -(fsc.lhs.giniImpurity()*lhs_size + fsc.lhs.giniImpurity()*rhs_size) / size
    }
}
// Typically want to minimize impurity, so negative.
class InformationGain : ImportanceStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        val lhs_size = fsc.lhs.size.toDouble()
        val rhs_size = fsc.rhs.size.toDouble()
        val size = lhs_size + rhs_size
        // entropy(parent) is a constant under comparison
        return -(fsc.lhs.entropy()*lhs_size + fsc.lhs.entropy()*rhs_size) / size
    }
}

data class TreeLearningParams(
        val fStats: List<StreamingStats>,
        val numSplitsPerFeature: Int=1,
        val minLeafSupport: Int=30,
        val strategy: ImportanceStrategy = InformationGain()
) {
    fun validFeatures(fids: Collection<Int>): List<Int> = fids.filter {
        val stats = fStats[it]
        stats.min != stats.max
    }
    fun isValid(fsc: FeatureSplitCandidate): Boolean {
        return fsc.lhs.size >= minLeafSupport && fsc.rhs.size >= minLeafSupport
    }
    fun estimateImportance(fsc: FeatureSplitCandidate): Double = strategy.importance(fsc)
}

data class RecursionTreeParams(val features: Set<Int>, val instances: Set<QDoc>, val depth: Int = 0) {
    val done: Boolean get() = features.isEmpty()
    fun choose(fsc: FeatureSplitCandidate): Pair<RecursionTreeParams, RecursionTreeParams> {
        val fnext = HashSet(features).apply { remove(fsc.fid) }
        return Pair(
            RecursionTreeParams(fnext, fsc.lhs.instances.toSet(), depth+1),
            RecursionTreeParams(fnext, fsc.rhs.instances.toSet(), depth+1))
    }
}

fun trainTreeRecursive(params: TreeLearningParams, step: RecursionTreeParams): TreeNode? {
    if (step.done) return null
    // if we can't possibly generate supported leaves:
    if (step.instances.size < params.minLeafSupport*2) return null

    val splits = params.validFeatures(step.features).flatMap { fid ->
        val stats = params.fStats[fid]
        val sortedInstances = step.instances.sortedBy { it.features[fid] }
        (0 until params.numSplitsPerFeature).map {
            FeatureSplitCandidate(fid,
                    ThreadLocalRandom.current().nextDouble(stats.min, stats.max)).apply {
                considerSorted(sortedInstances)
            }
        }
    }.filter { params.isValid(it) }
    if (splits.isEmpty()) return null

    val bestFeature = splits.maxBy { params.estimateImportance(it) } ?: return null

    val (lhsp, rhsp) = step.choose(bestFeature)
    val lhs = trainTreeRecursive(params, lhsp) ?: bestFeature.leftLeaf()
    val rhs = trainTreeRecursive(params, rhsp) ?: bestFeature.rightLeaf()
    return FeatureSplit(bestFeature.fid, bestFeature.split, lhs, rhs)
}


fun trainTree(params: TreeLearningParams, features: Collection<Int>, instances: Collection<QDoc>): TreeNode? = trainTreeRecursive(params, RecursionTreeParams(params.validFeatures(features).toSet(), instances.toSet()))
