package edu.umass.cics.ciir.learning

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.iltr.pagerank.SpacesRegex
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.getEvaluator
import edu.umass.cics.ciir.sprf.pmake
import gnu.trove.map.hash.TIntIntHashMap
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.QuerySetJudgments
import org.lemurproject.galago.core.eval.metric.QueryEvaluator
import org.lemurproject.galago.core.retrieval.ScoredDocument
import org.lemurproject.galago.utility.MathUtils
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.lists.Ranked
import java.io.File
import java.util.*
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ThreadLocalRandom


val mapper = ObjectMapper().registerModule(KotlinModule())


/**
 *
 * @author jfoley.
 */

fun Parameters.children(key: String): List<Parameters> = this.getAsList(key, Parameters::class.java)
fun loadTreeNode(p: Parameters): TreeNode {
    val tn: TreeNode = if (p.containsKey("ensemble")) {
        EnsembleNode(p.children("ensemble").map { loadTreeNode(it) })
    } else if (p.containsKey("point")) {
        FeatureSplit(p.getInt("fid"), p.getDouble("point"),
                loadTreeNode(p.getMap("lhs")), loadTreeNode(p.getMap("rhs")))
    } else {
        LeafResponse(p.getDouble("probability"), p.getDouble("accuracy"), p.getDouble("confidence"))
    }
    tn.apply { weight = p.get("weight", 1.0) }
    return tn
}
sealed class TreeNode {
    var weight: Double = 1.0
    abstract fun score(features: FloatArray): Double
    abstract fun depth(): Int
    abstract fun toParameters(): Parameters
}
data class EnsembleNode(val guesses: List<TreeNode>): TreeNode() {
    override fun depth(): Int = guesses.map { it.depth() }.max() ?: 0
    override fun score(features: FloatArray): Double = weight * guesses.meanByDouble { it.score(features) }
    override fun toParameters() = pmake {
        set("weight", weight)
        set("ensemble", guesses.map { it.toParameters() })
    }
}
data class FeatureSplit(val fid: Int, val point: Double, val lhs: TreeNode, val rhs: TreeNode): TreeNode() {
    override fun score(features: FloatArray): Double = if (features[fid] < point) {
        weight * lhs.score(features)
    } else {
        weight * rhs.score(features)
    }
    override fun depth(): Int = 1 + maxOf(lhs.depth(), rhs.depth())
    override fun toParameters() = pmake {
        set("weight", weight)
        set("fid", fid)
        set("point", point)
        set("lhs", lhs.toParameters())
        set("rhs", rhs.toParameters())
    }
}

data class LeafResponse(val probability: Double, val accuracy: Double = 1.0, val confidence: Double=1.0, val items: InstanceSet? = null) : TreeNode() {
    constructor(items: InstanceSet): this(items.output, items.accuracy, items.confidence, items)
    private val precomputed = accuracy * probability * confidence
    override fun score(features: FloatArray): Double = weight * precomputed
    override fun depth(): Int = 1
    override fun toParameters() = pmake {
        set("weight", weight)
        set("accuracy", accuracy)
        set("probability", probability)
        set("confidence", confidence)
        if (items != null) {
            set("observed", items.size)
            set("observed.labelCounts", items.labelCounts.toString())
        }
    }
}
data class LinearRankingLeaf(val fids: List<Int>, val weights: List<Double>): TreeNode() {
    override fun score(features: FloatArray): Double {
        var sum = 0.0
        fids.forEachIndexed { i, fid ->
            sum += weights[i] * features[fid]
        }
        return weight * MathUtils.sigmoid(sum)
    }
    override fun depth(): Int = 1
    override fun toParameters() = error("TODO")
}
data class LinearPerceptronLeaf(val fids: List<Int>, val weights: List<Double>): TreeNode() {
    override fun score(features: FloatArray): Double {
        var sum = 0.0
        fids.forEachIndexed { i, fid ->
            sum += weights[i] * features[fid]
        }
        val pred = if (sum >= 0.0) { 1.0 } else { -1.0 }
        return weight * pred
    }
    override fun depth(): Int = 1
    override fun toParameters() = error("TODO")
}

data class QDoc(val label: Float, val qid: String, val features: FloatArray, val name: String, var judged: Boolean = false) {
    private val identity: String = "$qid:$name"
    private val idHash = identity.hashCode()
    override fun hashCode(): Int = idHash
    override fun equals(other: Any?): Boolean {
        if (other is QDoc) {
            return idHash == other.idHash && identity == other.identity
        }
        return false
    }
    val binaryLabel: Int = if (label > 0) { 1 } else { 0 }
    val perceptronLabel: Int = if (label > 0) { 1 } else { -1 }
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

fun loadRLDataset(input: String, qrels: QuerySetJudgments, numFeatures: Int): RLDataset {
    val byQuery = HashMap<String, MutableList<QDoc>>()

    val inputF = File(input)
    val cborInput = File("$input.cbor.gz")
    if (cborInput.exists()) {
        cborInput.smartReader().use { br ->
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

        cborInput.smartWriter().use { ow ->
            mapper.writeValue(ow, RLDataset(byQuery))
        }
    }

    byQuery.forEach { qid, docs ->
        val judgments = qrels[qid]
        if (judgments != null) {
            docs.forEach {
                if (judgments.isJudged(it.name)) {
                    it.judged = true
                }
            }
        }
    }

    return RLDataset(byQuery)
}

object TestModel {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val dataset = argp.get("dataset", "gov2")
        val modelFile = File("mq07.rf.json")
        if (!modelFile.exists()) error("Need input $modelFile!")
        val tree = loadTreeNode(Parameters.parseFile(modelFile))

        val input = argp.get("input", "l2rf/${dataset}.features.ranklib")

        val featureNames = Parameters.parseFile(argp.get("meta", "$input.meta.json"))
        val numFeatures = argp.get("numFeatures",
                featureNames.size)
        val querySet = DataPaths.get(dataset)
        val queries = querySet.title_qs
        val qrels = querySet.qrels
        val measure = getEvaluator("ap")

        val byQuery = loadRLDataset(input, qrels, numFeatures).byQuery

        val meanMeasure = byQuery.toList().meanByDouble { (qid, inputList) ->
            val ranked = inputList.mapTo(ArrayList()) {
                val pred = tree.score(it.features)
                ScoredDocument(it.name, -1, pred)
            }
            Ranked.setRanksByScore(ranked)
            val score = measure.evaluate(QueryResults(ranked), qrels[qid] ?: QueryJudgments(qid, emptyMap()))
            println("%s %1.3f".format(qid, score))
            score
        }

        println("Overall %1.3f".format(meanMeasure))
    }
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dataset = argp.get("dataset", "mq07")
    val outputFile = File("${dataset}.rf.json")
    if (outputFile.exists()) error("Output $outputFile already exists!")
    val input = argp.get("input", "l2rf/${dataset}.features.ranklib")
    val featureNames = Parameters.parseFile(argp.get("meta", "$input.meta.json"))
    val numFeatures = argp.get("numFeatures",
            featureNames.size)
    val numTrees = argp.get("numTrees", 50)
    val sampleRate = argp.get("srate", 0.25)
    val featureSampleRate = argp.get("frate", 0.05)
    val kSplits = argp.get("kcv", 5)
    val kFeatures = argp.get("numFeatures", (featureSampleRate * numFeatures).toInt())

    val querySet = DataPaths.get(dataset)
    val queries = querySet.title_qs
    val qrels = querySet.qrels

    //val (queries, qrels) = loadTrecCarDataset(File("data/trec-car-train-10k.qrels"))
    val measure = getEvaluator("ap")
    val strategy = getTreeSplitSelectionStrategy(argp.get("strategy", "conf-variance"))


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

        println("Train ${trainQs.size} Validate: ${valiQs.size} Test ${testQs.size}.")
        CVSplit(index, trainQs, valiQs, testQs)
    }

    println("numFeatures: $numFeatures numSplits: $kSplits")
    val byQuery = loadRLDataset(input, qrels, numFeatures).byQuery

    val splitPerf = splits.pmapIndividual(ForkJoinPool(kSplits+1)) { split ->
        val trainInsts = split.trainIds.flatMap { byQuery[it]?.toList() ?: emptyList() }
        val trainFStats = (0 until numFeatures).map { StreamingStats() }
        val kSamples = argp.get("numSamples", (sampleRate * trainInsts.size).toInt())
        println("${split.id} kSamples=$kSamples kFeatures=$kFeatures")

        trainInsts.forEach { doc ->
            doc.features.forEachIndexed { fid, fval -> trainFStats[fid].push(fval.toDouble()) }
        }
        val learningParams = TreeLearningParams(trainFStats.map { it.toComputedStats() }, strategy = strategy)

        if (kSamples <= 1) {
            error("Cannot function with few samples. Only selects $kSamples in practice.")
        }

        val trainSet = trainInsts.groupBy { it.qid }
        val testSet = split.testIds.flatMap { byQuery[it]?.toList() ?: emptyList() }.groupBy { it.qid }
        val valiSet = split.valiIds.flatMap { byQuery[it]?.toList() ?: emptyList() }.groupBy { it.qid }

        val outputTrees = ArrayList<TreeNode>()
        val rand = Random(13)

        while(outputTrees.size < numTrees) {
            val f_sample = (0 until numFeatures).sample(kFeatures, rand).toList()
            val x_sample = trainInsts.sample(kSamples, rand).toList()
            //println(f_sample)

            //val outOfBag = HashSet(trainInsts).apply { removeAll(x_sample) }.groupBy { it.qid }

            val positives = x_sample.count { it.perceptronLabel > 0 }
            //println("Positives: $positives/${x_sample.size}")
            if (positives == 0) {
                println("Note: bad sample, no positive labels in sample.")
                continue
            }

            learningParams.features = f_sample
            val tree = trainTree(learningParams, f_sample, x_sample)
            if (tree == null) {
                println("Could not learn a tree from this sample...")
                continue
            }
            //println("Learned tree $tree")

            //val trainAP = split.evaluate(trainSet, measure, qrels, tree)
            //val oobAP = split.evaluate(outOfBag, measure, qrels, tree)
            //val valiAP = split.evaluate(valiSet, measure, qrels, tree)
            //println("\ttrain-AP: $trainAP, oob-AP: $oobAP, vali-AP: $valiAP")

            //tree.weight = oobAP
            outputTrees.add(tree)
            //println(tree.toParameters().toPrettyString())

            if (outputTrees.size >= 1) {
                val ensemble = EnsembleNode(outputTrees)
                val trainAP = split.evaluate(trainSet, measure, qrels, ensemble)
                val valiAP = split.evaluate(valiSet, measure, qrels, ensemble)
                val testAP = split.evaluate(testSet, measure, qrels, ensemble)
                synchronized(System.out) {
                    println("${split.id} ENSEMBLE[]%d.d=%d.w=%1.3f train-AP: %1.3f, vali-AP: %1.3f, test-AP: %1.3f".format(outputTrees.size, tree.depth(), tree.weight, trainAP, valiAP, testAP))
                }
                //println("ENSEMBLE[]${outputTrees.size} train-AP: $trainAP, vali-AP: $valiAP, test-AP: $testAP")
            }
        }

        val ensemble = EnsembleNode(outputTrees)
        val testAP = split.evaluate(testSet, measure, qrels, ensemble)

        println("Split: ${split.id} Test-AP: ${"%1.3f".format(testAP)}")
        Pair(split.id, Pair(ensemble, testAP))
    }.associate { it }

    val overallTestAP = splitPerf.values.map { it.second }.toList().mean()
    println("Overall Test-AP: ${"%1.3f".format(overallTestAP)}")
    println(splitPerf.mapValues { (_,v) -> v.second })

    // Only use this if you have another, true test set.
    val overallEnsemble = EnsembleNode(splitPerf.values.flatMap { it.first.guesses })
    outputFile.smartPrint { writer ->
        writer.println(overallEnsemble.toParameters())
    }
}

data class QDocFeatureView(val doc: QDoc, val features: List<Int>) : Vector {
    override val dim: Int get() = features.size
    override fun get(i: Int): Double = doc.features[features[i]].toDouble()
}

class InstanceSet : MachineLearningInput {
    lateinit var features: List<Int>
    val forLearning: ArrayList<QDocFeatureView> by lazy { instances.mapTo(ArrayList()) { QDocFeatureView(it, features) } }
    override val numInstances: Int get() = instances.size
    override val numFeatures: Int get() = features.size
    override fun shuffle() { forLearning.shuffle() }
    override fun get(i: Int): Vector = forLearning[i]
    override fun truth(i: Int): Boolean = forLearning[i].doc.label > 0.0

    val instances = ArrayList<QDoc>()
    val labelStats = StreamingStats()
    val size: Int get() = instances.size
    val output: Double get() = safeDiv(labelCounts[1], size)
    val labelCounts = TIntIntHashMap()
    val perceptronLabel: Int get() = if (output > 0) { 1 } else { -1 }

    val accuracy: Double get() = safeDiv(labelCounts[perceptronLabel], size)
    val confidence: Double get() = safeDiv(instances.count { it.judged }, size)

    override fun equals(other: Any?): Boolean {
        if (other is InstanceSet) {
            return instances == other.instances
        }
        return false
    }

    fun push(x: QDoc) {
        val label = x.perceptronLabel
        labelCounts.adjustOrPutValue(label, 1, 1)
        labelStats.push(label)
        instances.add(x)
    }
    fun pushAll(x: Collection<QDoc>): InstanceSet {
        x.forEach {
            val label = it.perceptronLabel
            labelStats.push(label.toDouble())
            labelCounts.adjustOrPutValue(label, 1, 1)
        }
        instances.addAll(x)
        return this
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

data class FeatureSplitCandidate(
        val fid: Int, val split: Double,
        val lhs: InstanceSet = InstanceSet(),
        val rhs: InstanceSet = InstanceSet()) {
    var cachedImportance: Double? = null
    fun consider(instances: Collection<QDoc>) {
        for (inst in instances) {
            if (inst.features[fid] > split) {
                rhs.push(inst)
            } else {
                lhs.push(inst)
            }
        }
    }
    fun leftLeaf(params: TreeLearningParams): TreeNode = params.makeOutputNode(lhs)
    fun rightLeaf(params: TreeLearningParams): TreeNode = params.makeOutputNode(rhs)
}

data class TreeLearningParams(
        val fStats: List<ComputedStats>,
        val numSplitsPerFeature: Int=3,
        val minLeafSupport: Int=10,
        val maxDepth: Int = 8,
        val rankerLeaf: Boolean = false,
        val perceptronLeaf: Boolean = false,
        val perceptronMaxIters: Int = 100,
        val useFeaturesOnlyOnce: Boolean = false,
        val splitter: SplitGenerationStrategy = EvenSplitGenerator(),
        val strategy: TreeSplitSelectionStrategy = TrueVarianceReduction()
) {
    var bagInstances: Set<QDoc> = emptySet()
    fun validFeatures(fids: Collection<Int>): List<Int> = fids.filter {
        val stats = fStats[it]
        stats.min != stats.max
    }
    fun isValid(fsc: FeatureSplitCandidate): Boolean {
        return fsc.lhs.size >= minLeafSupport && fsc.rhs.size >= minLeafSupport
    }
    fun estimateImportance(fsc: FeatureSplitCandidate): Double {
        fsc.cachedImportance?.let { found -> return found }
        val computed = strategy.importance(fsc)
        fsc.cachedImportance = computed
        return computed
    }
    var features: List<Int> = Collections.emptyList()
    fun makeOutputNode(elements: InstanceSet): TreeNode {
        val accuracy = elements.accuracy
        if (accuracy > 0.95) {
            return LeafResponse(elements)
        }

        if (rankerLeaf) {
            elements.features = features
            val ca = CoordinateAscentRanker(elements)
            val weights = ca.learn().copy()
            weights.normalizeL2()
            return LinearRankingLeaf(features, weights.toList()).apply {
                weight = elements.accuracy * elements.confidence
            }
        } else if (perceptronLeaf) {
            if (elements.labelStats.variance > 0 && accuracy < 0.8) {
                elements.features = features
                val learned = learnAveragePerceptron(elements, maxIters = perceptronMaxIters)
                val p_acc = learned.accuracy
                val e_acc = accuracy
                if (learned.informative && (learned.converged || p_acc > e_acc)) {
                    println("Choose Perceptron. Accuracy of $p_acc vs $e_acc")
                    return LinearPerceptronLeaf(features, learned.weights.toList())
                }
            }
            return LeafResponse(elements)
        } else {
            return LeafResponse(elements)
        }
    }

    fun reset(instances: Set<QDoc>) {
        bagInstances = instances
    }

}

data class RecursionTreeParams(val features: Set<Int>, val instances: Set<QDoc>, val depth: Int = 1) {
    val done: Boolean get() = features.isEmpty()
    fun choose(params: TreeLearningParams, fsc: FeatureSplitCandidate): Pair<RecursionTreeParams, RecursionTreeParams> {
        val fnext = if (params.useFeaturesOnlyOnce) {
            HashSet(features).apply { remove(fsc.fid) }
        } else features
        return Pair(
            RecursionTreeParams(fnext, fsc.lhs.instances.toSet(), depth+1),
            RecursionTreeParams(fnext, fsc.rhs.instances.toSet(), depth+1))
    }
}

interface SplitGenerationStrategy {
    fun generateSplits(params: TreeLearningParams, stats: ComputedStats, fid: Int, instances: Collection<QDoc>): List<FeatureSplitCandidate>
    fun rand(min: Double, max: Double) = ThreadLocalRandom.current().nextDouble(min, max)
}
class ExtraRandomForestSplitGenerator : SplitGenerationStrategy {
    override fun generateSplits(params: TreeLearningParams, stats: ComputedStats, fid: Int, instances: Collection<QDoc>): List<FeatureSplitCandidate> {
        val actualStats = StreamingStats().pushAll(instances.map { it.features[fid].toDouble() })
        if (actualStats.min == actualStats.max) return emptyList()
        return (0 until params.numSplitsPerFeature).map {
            FeatureSplitCandidate(fid, rand(actualStats.min, actualStats.max)).apply {
                consider(instances)
            }
        }
    }
}
class EvenSplitGenerator : SplitGenerationStrategy {
    override fun generateSplits(params: TreeLearningParams, fidStats: ComputedStats, fid: Int, instances: Collection<QDoc>): List<FeatureSplitCandidate> {
        val stats = StreamingStats()
        instances.forEach() { stats.push(it.features[fid]) }
        val k = params.numSplitsPerFeature

        val range = stats.max - stats.min
        return (0 until k).map { i ->
            val frac = safeDiv(i, k-1)
            val split = frac * range + stats.min

            FeatureSplitCandidate(fid, split).apply { consider(instances) }
        }
    }
}

fun trainTreeRecursive(params: TreeLearningParams, step: RecursionTreeParams): TreeNode? {
    //val indent = (0 until step.depth).joinToString(separator="") { "\t" }
    //println("${indent}TTR: ${step.depth}, N=${step.instances.size}")

    // Limit recursion depth according to parameters.
    if (step.depth >= params.maxDepth) return null
    // Stop if we run out of features in this sample.
    if (step.done) return null
    // if we can't possibly generate supported leaves:
    if (step.instances.size < params.minLeafSupport*2) return null

    val splits = params.validFeatures(step.features).flatMap { fid ->
        val stats = params.fStats[fid]
        params.splitter.generateSplits(params, stats, fid, step.instances)
    }.filter { params.isValid(it) }
    if (splits.isEmpty()) return null

    val bestFeature = splits.maxBy { params.estimateImportance(it) } ?: return null

    val (lhsp, rhsp) = step.choose(params, bestFeature)
    val lhs = trainTreeRecursive(params, lhsp) ?: bestFeature.leftLeaf(params)
    val rhs = trainTreeRecursive(params, rhsp) ?: bestFeature.rightLeaf(params)
    return FeatureSplit(bestFeature.fid, bestFeature.split, lhs, rhs)
}


fun trainTree(params: TreeLearningParams, features: Collection<Int>, instances: Collection<QDoc>): TreeNode? {
    params.reset(instances.toSet())
    return trainTreeRecursive(params, RecursionTreeParams(params.validFeatures(features).toSet(), instances.toSet()))
}

