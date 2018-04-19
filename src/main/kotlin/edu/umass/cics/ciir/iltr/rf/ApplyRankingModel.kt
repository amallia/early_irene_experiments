package edu.umass.cics.ciir.iltr.rf

import com.linkedin.paldb.api.Configuration
import com.linkedin.paldb.api.PalDB
import com.linkedin.paldb.api.StoreWriter
import edu.umass.cics.ciir.iltr.LTRQuery
import edu.umass.cics.ciir.iltr.loadRanklibViaJSoup
import edu.umass.cics.ciir.irene.galago.NamedMeasures
import edu.umass.cics.ciir.irene.galago.getEvaluator
import edu.umass.cics.ciir.irene.galago.incr
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.ltr.toRRExpr
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import edu.umass.cics.ciir.irene.utils.*
import edu.umass.cics.ciir.learning.*
import edu.umass.cics.ciir.learning.Vector
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.eval.EvalDoc
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.SimpleEvalDoc
import org.lemurproject.galago.utility.Parameters
import org.roaringbitmap.RoaringBitmap
import java.io.File
import java.util.*
import kotlin.collections.HashMap
import kotlin.coroutines.experimental.buildSequence

/**
 *
 * @author jfoley.
 */
data class RanklibInput(val label: Double, val qid: String, val name: String, val features: FloatArrayVector, var prediction: Double = Double.NaN) : WeightedForHeap {
    val relevant: Boolean get() = label > 0
    override val weight: Float get() = prediction.toFloat()
    var pvec: FloatArrayVector? = null
    fun toEvalDoc(rank: Int) = SimpleEvalDoc(name, rank, prediction)
    // Binary representation of activated leaf nodes.
    var bvec: RoaringBitmap? = null
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

inline fun <R> StoreWriter.use(block: (StoreWriter)->R) {
    var closed = false
    try {
        val result = block(this)
        closed = true
        this.close()
    } finally {
        if (!closed) {
            this.close()
        }
    }
}

object CreatePalDBDocCache {
    @JvmStatic val config = PalDB.newConfiguration().apply {
        set(Configuration.COMPRESSION_ENABLED, "true")
    }
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val data = File(argp.get("dir", "nyt-cite-ltr"))
        val dsName = argp.get("dataset", "trec-core")
        val dataset = DataPaths.get(dsName)
        val index = dataset.getIreneIndex()
        val ranklibInputName = "${dsName}.features.ranklib.gz"
        val meta = Parameters.parseFile(findMetaFile(File(data, ranklibInputName)))

        val names = genRanklibInputs(File(data, ranklibInputName), meta.size+1).map { it.name }.toHashSet()
        println("Found ${names.size} documents to cache.")

        val msg = CountingDebouncer(total = names.size.toLong())
        PalDB.createWriter(File("data/$dsName.paldb"), config).use { storeWriter ->
            names.parallelStream().map { name ->
                val num = index.documentById(name) ?: return@map null
                val json = index.docAsParameters(num) ?: return@map null
                Pair(name, json.toString())
            }.sequential().forEach { kv ->
                if (kv != null) {
                    val (name, json) = kv
                    storeWriter.put(name, json);
                }
                msg.incr()?.let { upd ->
                    println(upd)
                }
            }
        }
        println("Finished writing cache.")
    }
}

data class FBRankDoc(val label: Double, val name: String, val features: HashMap<String, Double>)

class ForestBinaryRepr(val model: TreeNode) {
    val leafNumbers = IdentityHashMap<TreeNode, Int>()
    init {
        var i = 0
        model.visit { node ->
            when (node) {
                is EnsembleNode,
                is FeatureSplit -> { }

                is LeafResponse,
                is LinearRankingLeaf,
                is LinearPerceptronLeaf -> {
                    leafNumbers[node] = i++
                }
            }
        }
    }

    private fun score(node: TreeNode, features: Vector, onLeafFn: (Int)->Unit) {
        when(node) {
            is EnsembleNode -> {
                for (c in node.guesses) {
                    score(c, features, onLeafFn)
                }
            }
            is FeatureSplit -> {
                if (features[node.fid] < node.point) {
                    score(node.lhs, features, onLeafFn)
                }
            }
            is LeafResponse,
            is LinearRankingLeaf,
            is LinearPerceptronLeaf -> {
                val num = leafNumbers[node]
                if (num != null) {
                    onLeafFn(num)
                }
            }
        }
    }

    fun predict(features: Vector): RoaringBitmap {
        val bitmap = RoaringBitmap()
        score(model, features, bitmap::add)
        return bitmap
    }
}

fun computeSimilarities(dim: Int, cand: RoaringBitmap, group: List<RoaringBitmap>): StreamingStats {
    val ss = StreamingStats()
    for (g in group) {
        ss.push(cand.hammingSimilarity(dim, g))
    }
    return ss
}

fun RoaringBitmap.hammingSimilarity(dim: Int, other: RoaringBitmap?): Double {
    if (other == null) return 0.0
    val copy = this.clone()
    copy.xor(other)
    val hammingCount = copy.cardinality
    // 1.0 - fraction of bits different == fraction of bits the same
    return 1.0 - safeDiv(hammingCount, dim)
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val data = File(argp.get("dir", "nyt-cite-ltr"))
    val dsName = argp.get("dataset", "gov2")
    val dataset = DataPaths.get(dsName)
    val qrels = dataset.qrels
    val map = getEvaluator("map")
    val numFolds = argp.get("folds", 5)
    val includeRawLTRFeatures = argp.get("includeRawLTRFeatures", false)
    val includeOriginal = argp.get("includeOriginal", false)
    val includeStacked = argp.get("includeStacked", true)
    val includeTrees = argp.get("includeTrees", false)
    val modelName = when(dsName) {
        "trec-core" -> "lm.t200.l32"
        "gov2" -> "mq07.lambdaMart.l10.kcv10.tvs90.gz"
        else -> error("Model name for $dataset")
    }
    val depth = argp.get("depth", 10)
    val p10 = getEvaluator("p$depth")

    // keep track of the number of relevant documents for calculating MAP ourselves (if needed).
    val nRels = qrels.mapValues { (_,vals) -> vals.relevantJudgmentCount }

    val index = dataset.getIreneIndex()
    index.env.estimateStats = "min"
    val docCache = PalDB.createReader(File("data/$dsName.paldb"), CreatePalDBDocCache.config)
    val getDoc: (String)->LTRDoc = { name ->
        val fjson = Parameters.parseString(docCache.getString(name, "{}"))
        LTRDoc.create(name, fjson, dataset.textFields, index)
    }

    val ranklibInputName = "${dsName}.features.ranklib.gz"
    val meta = Parameters.parseFile(findMetaFile(File(data, ranklibInputName)))
    val featureNames = meta.keys.associate { Pair(meta.getInt(it), it) }

    println(meta)
    // Just add a buffer since Ranklib features are off-by-one.
    val dim = meta.size + 1
    println("Dim: $dim")

    val model: TreeNode = loadRanklibViaJSoup(File(data, modelName).absolutePath)
    val brepr = ForestBinaryRepr(model)
    val numTrees = (model as EnsembleNode).guesses.size
    println("Num Trees: $numTrees")
    val numLeaves = brepr.leafNumbers.size
    println("Num Leafs: $numLeaves")

    val queries = HashMap<String, List<RanklibInput>>(250)
    genRanklibQueries(File(data, ranklibInputName), dim).forEach { docs ->
        val qid = docs[0].qid
        println(" -> $qid ${docs.size} ${docs.count { it.relevant }}")
        // Predict via model.
        docs.forEach {
            val predv = model.predictionVector(it.features.data)
            it.prediction = predv.mean()
            it.pvec = FloatArrayVector(predv.map { it.toFloat() }.toFloatArray())
            it.bvec = brepr.predict(it.features)
            //it.prediction = model.score(it.features.data)
            //it.prediction = it.features[162]
            //it.prediction = it.features[79] // sdm-stop for gov2
        }
        // sort/rank by the model
        queries[qid] = docs.sortedByDescending { it.prediction }
    }

    val user = queries.mapValues { (_, docs) -> InteractionFeedback(docs, depth) }

    val fbCounts = HashMap<Int,Int>()
    user.forEach { qid, fb ->
        fbCounts.incr(fb.correct.size, 1)
    }

    val fbField = argp.get("fbField", index.defaultField)
    val fbTerms = argp.get("fbTerms", 100)

    val modelFusion = HashMap<String, List<FBRankDoc>>()

    val measures = NamedMeasures()
    val msg = CountingDebouncer(user.size.toLong())
    user.forEach { qid, fb ->
        val qtext = dataset.title_qs[qid] ?: ""
        val qterms = index.tokenize(qtext, fbField)

        val judgments = qrels[qid] ?: QueryJudgments(qid, emptyMap())
        val ranking = fb.docs.mapIndexed { i, rli -> rli.toEvalDoc(i+1) }
        println("$qid\t\t$measures")

        val fbRawPos = computeMeanVector(fb.correct.map { it.features })
        val fbRawNeg = computeMeanVector(fb.incorrect.map { it.features })
        val fbLTRPos = computeMeanVector(fb.correct.map { it.pvec!! })
        val fbLTRNeg = computeMeanVector(fb.incorrect.map { it.pvec!! })

        val correctDocs = fb.correct.map { getDoc(it.name) }
        val incorrectDocs = fb.incorrect.map { getDoc(it.name) }

        val ranklib = fb.remaining.associateBy { it.name }
        val targets = LTRQuery(qid, qtext, qterms, fb.remaining.map { getDoc(it.name) })

        // Construct Rocchio-like:
        val posTFModel = HashMap<String, Double>()
        val posRMModel = HashMap<String, Double>()
        correctDocs.forEach { doc ->
            val dist = doc.freqs(fbField)
            dist.counts.forEachEntry { term, weight ->
                if (!inqueryStop.contains(term)) {
                    posTFModel.incr(term, weight.toDouble())
                    posRMModel.incr(term, weight.toDouble() / dist.length)
                }
                true
            }
        }
        val negTFModel = HashMap<String, Double>()
        incorrectDocs.forEach { doc ->
            val dist = doc.freqs(fbField)
            dist.counts.forEachEntry { term, weight ->
                if (!inqueryStop.contains(term)) {
                    negTFModel.incr(term, weight.toDouble())
                }
                true
            }
        }

        // Relevance Feedback Models as features:
        val exprs = dataset.textFields.flatMap { field ->
            listOf(
                    // RM (fixed-prior)
                    Pair("fbnorm:$field:rm-dir", weightedNormalizedQuery(posRMModel, fbTerms, fbField, {DirQLExpr(it)})),
                    Pair("fbnorm:$field:rm-bm25", weightedNormalizedQuery(posRMModel, fbTerms, fbField, { BM25Expr(it)} )),
                    // gamma from Rocchio
                    Pair("fbnorm:$field:tf-neg-dir", weightedNormalizedQuery(negTFModel, fbTerms, fbField, {DirQLExpr(it)})),
                    Pair("fbnorm:$field:tf-neg-bm25", weightedNormalizedQuery(negTFModel, fbTerms, fbField, { BM25Expr(it)} )),
                    // beta from Rocchio
                    Pair("fbnorm:$field:tf-pos-dir", weightedNormalizedQuery(posTFModel, fbTerms, fbField, {DirQLExpr(it)})),
                    Pair("fbnorm:$field:tf-pos-bm25", weightedNormalizedQuery(posTFModel, fbTerms, fbField, { BM25Expr(it)} ))
            ).filterNot { it.second == null }
        }.associate { Pair(it.first, it.second!!.toRRExpr(index.env)) } // Turn into map.

        val fstats = HashMap<String, StreamingStats>()
        val stackedFeatures = targets.docs.map { doc ->
            val rlib = ranklib[doc.name]!!
            val features = HashMap<String, Double>()

            // convert exprs to features:
            features.putAll(exprs.mapValues { (_, scorer) -> scorer.eval(doc) })
            features.put("fbRelevant", safeDiv(fb.correct.size, depth))

            // Mean vector features...
            features.put("fbPosLTRDot", fbLTRPos?.dotp(rlib.pvec!!) ?: 0.0)
            features.put("fbPosLTRCos", fbLTRPos?.cosineSimilarity(rlib.pvec!!) ?: 0.0)
            features.put("fbNegLTRDot", fbLTRNeg?.dotp(rlib.pvec!!) ?: 0.0)
            features.put("fbNegLTRCos", fbLTRNeg?.cosineSimilarity(rlib.pvec!!) ?: 0.0)

            val posSim = computeSimilarities(numLeaves, rlib.bvec!!, fb.correct.map { it.bvec!! })
            val negSim = computeSimilarities(numLeaves, rlib.bvec!!, fb.correct.map { it.bvec!! })
            features.putAll(posSim.features.mapKeys { (k, _) -> "fbBinPos:$k" })
            features.putAll(negSim.features.mapKeys { (k, _) -> "fbBinNeg:$k" })

            // KNN-style features (mean of distances) will be different from the distance to the mean.
            val posV = VectorGroup(fb.correct.map { it.pvec!! })
            val negV = VectorGroup(fb.incorrect.map { it.pvec!! })
            features.putAll(posV.dotProductStats(rlib.pvec!!).features.mapKeys { (k,_) -> "fbPos:$k" })
            features.putAll(negV.dotProductStats(rlib.pvec!!).features.mapKeys { (k,_) -> "fbNeg:$k" })

            if (includeRawLTRFeatures) {
                features.put("fbPosRawDot", fbRawPos?.dotp(rlib.features) ?: 0.0)
                features.put("fbPosRawCos", fbRawPos?.cosineSimilarity(rlib.features) ?: 0.0)
                features.put("fbNegRawDot", fbRawNeg?.dotp(rlib.features) ?: 0.0)
                features.put("fbNegRawCos", fbRawNeg?.cosineSimilarity(rlib.features) ?: 0.0)
            }

            if (includeOriginal) {
                rlib.features.data.forEachIndexed { i, score ->
                    val fname = featureNames[i] ?: return@forEachIndexed
                    features.put(fname, score.toDouble())
                }
            }

            if (includeTrees) {
                rlib.pvec!!.data.forEachIndexed { i, score ->
                    features.put("firstPass[$i]", score.toDouble())
                }
            }

            if (includeStacked) {
                features.put("firstPass[mean]", rlib.prediction)
            }

            // Collect stats to normalize any features requesting it:
            features.forEach { feature, score ->
                if (feature.startsWith("fbnorm:")) {
                    fstats.computeIfAbsent(feature, { StreamingStats() }).push(score)
                }
            }

            FBRankDoc(rlib.label, doc.name, features)
        }

        // finish normalization:
        stackedFeatures.forEach { doc ->
            val delta = doc.features.mapValues { (fname, fval) ->
                fstats[fname]?.maxMinNormalize(fval) ?: fval
            }
            doc.features.clear()
            doc.features.putAll(delta)
        }

        modelFusion.put(qid, stackedFeatures)

        val noChangeRanking = QueryResults(qid, ranking.drop(depth))
        measures.ppush(qid,"AP[$depth..]", map.evaluate(noChangeRanking, judgments))

        // unsupervised LTR feedback
        if (fbRawPos != null) {
            val fbRaw_ranking = QueryResults(qid, fb.rankRemaining { fbRawPos.dotp(it.features) })
            val fbLTR_ranking = fb.rankRemaining { fbLTRPos!!.dotp(it.pvec!!) }
            measures.ppush(qid, "FB-RAW-AP[$depth..]", map.evaluate(QueryResults(qid, fbRaw_ranking), judgments))
            measures.ppush(qid, "FB-LTR-AP[$depth..]", map.evaluate(QueryResults(qid, fbLTR_ranking), judgments))
        } else {
            measures.push("FB-RAW-AP[$depth..]", map.evaluate(noChangeRanking, judgments))
            measures.push("FB-LTR-AP[$depth..]", map.evaluate(noChangeRanking, judgments))
        }

        // regular!
        measures.ppush(qid,"AP[0..]", map.evaluate(QueryResults(qid, ranking), judgments))
        measures.ppush(qid,"AP[20..]", map.evaluate(QueryResults(qid, ranking.drop(20)), judgments));
        measures.ppush(qid,"P$depth", p10.evaluate(QueryResults(qid, ranking), judgments));

        msg.incr()?.let { upd ->
            println(upd)
        }
    }

    // assign nice feature ids to all our features..
    val uniqFeatures: HashSet<String> = modelFusion.flatMapTo(HashSet<String>()) { (_, fdocs) ->
        fdocs.flatMapTo(HashSet<String>()) { it.features.keys }
    }

    // Write "new" meta file now.
    val newMeta = uniqFeatures.toList().sorted().mapIndexed { i, name -> Pair(name, i+1) }.associate { it }
    File(data, "$dsName.fb$depth.ranklib.features.meta.json").smartPrint { out ->
        out.println(Parameters.wrap(newMeta).toPrettyString())
    }

    // Actually write output file now.
    File(data, "$dsName.fb$depth.ranklib.features.gz").smartPrint { out ->
        modelFusion.forEach { qid, docs ->
            docs.forEach { instance ->
                val pt = newMeta.entries.associate { (fname, fid) ->
                    val fval = instance.features[fname] ?: 0.0
                    Pair(fid, fval)
                }.toSortedMap().entries
                        .joinToString(
                                separator = " ",
                                prefix = "${instance.label} qid:$qid ",
                                postfix = " #${instance.name}"
                        ) { (fid, fval) -> "$fid:$fval" }
                out.println(pt)
                if (instance.label > 0) {
                    println(pt)
                }
            }
        }
    }


    println(measures)
    println(fbCounts)
    index.close()
}

fun weightedNormalizedQuery(weights: Map<String, Double>, k: Int, field: String, scorer: (TextExpr)->QExpr): QExpr? {
    if (k <= 0) error("Must take *some* terms from weights: k=$k")
    if (weights.isEmpty()) return null
    val topK = ScoringHeap<WeightedWord>(k)
    weights.forEach { t, s -> topK.offer(s.toFloat(), { WeightedWord(s.toFloat(), t) }) }

    return SumExpr(topK.sorted
            .associate { Pair(it.word, it.weight.toDouble()) }
            .normalize()
            .map { scorer(TextExpr(it.key, field)).weighted(it.value) })
}

data class WeightedRanklibInput(override val weight: Float, val original: RanklibInput) : WeightedForHeap

data class InteractionFeedback(val correct: List<RanklibInput>, val incorrect: List<RanklibInput>, val remaining: List<RanklibInput>) {
    constructor(ranking: List<RanklibInput>, depth: Int) : this(ranking.take(depth).filter { it.relevant }, ranking.take(depth).filterNot { it.relevant }, ranking.drop(depth))
    val docs: List<RanklibInput> get() = (correct + incorrect + remaining).sortedByDescending { it.prediction }
    fun rankRemaining(scorer: (RanklibInput)->Double): List<EvalDoc> {
        val output = ScoringHeap<WeightedRanklibInput>(remaining.size)
        remaining.forEach { rdoc ->
            output.offer(WeightedRanklibInput(scorer(rdoc).toFloat(), rdoc))
        }
        return output.sorted.mapIndexed { i, sri -> SimpleEvalDoc(sri.original.name, i+1, sri.weight.toDouble()) }
    }
}