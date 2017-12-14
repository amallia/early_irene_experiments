package edu.umass.cics.ciir.wordvec

import edu.umass.cics.ciir.chai.*
import edu.umass.cics.ciir.iltr.pagerank.SpacesRegex
import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.irene.toQueryResults
import edu.umass.cics.ciir.learning.Vector
import edu.umass.cics.ciir.learning.computeMeanVector
import edu.umass.cics.ciir.sprf.*
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.MathUtils
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.FloatBuffer
import java.util.concurrent.ConcurrentHashMap
import kotlin.streams.asStream

/**
 * @author jfoley
 */

inline fun <T> boundsCheck(i: Int, N: Int, fn: ()->T): T {
    if (i < 0 || i >= N) error("Out of bounds error: $i $N.")
    return fn()
}

class FloatBufferVector(override val dim: Int, val buffer: FloatBuffer, val offset: Int) : Vector {
    override fun get(i: Int): Double = boundsCheck(i, dim) { buffer[offset+i].toDouble() }
    override fun toString(): String = "FBV${offset}"
}

class FloatVectorBuffer(val dim: Int, val n: Int) {
    val request = dim * n * 4
    val buffer = ByteBuffer.allocateDirect(dim * n * 4).asFloatBuffer()

    operator fun get(index : Int): FloatBufferVector = boundsCheck(index, n) {
        FloatBufferVector(dim, buffer, dim * index)
    }

    internal fun put(index: Int, data: FloatArray) {
        val offset = index*dim
        for (i in (0 until dim)) {
            buffer.put(offset + i, data[i])
        }
    }
}

interface Word2VecDB {
    val bgVector: Vector
    val dim: Int
    val N: Int
    operator fun get(term: String): Vector?
}

class InMemoryWord2VecDB(val terms: Map<String, Int>, val buffer: FloatVectorBuffer) : Word2VecDB {
    override val bgVector = buffer[0].copy()
    override val dim: Int = buffer.dim
    override val N: Int = terms.size
    override operator fun get(term: String): Vector? {
        val index = terms[term] ?: return null
        return buffer[index]
    }
}

fun loadWord2VecTextFormat(input: File): Word2VecDB {
    return input.smartReader().use { reader ->
        val firstLine = reader.readLine() ?: error("Empty file=$input")

        val cols = firstLine.split(SpacesRegex)
        val numVectors = cols[0].toInt()
        val numDimensions = cols[1].toInt()

        // Allocation may fail here:
        try {
            val storage = FloatVectorBuffer(numDimensions, numVectors)
            val terms = ConcurrentHashMap<String, Int>(numVectors)

            val msg = CountingDebouncer(numVectors.toLong())
            reader.lineSequence().mapIndexed {i, line -> Pair(i, line)}.asStream().parallel().forEach { (lineNo,line) ->
                val local = FloatArray(numDimensions)
                val items = line.split(' ')
                val term = items[0]
                for (i in (0 until numDimensions)) {
                    local[i] = items[i+1].toFloat()
                }
                storage.put(lineNo, local)
                terms.put(term, lineNo)

                msg.incr()?.let { upd ->
                    println("loadWord2VecTextFormat $input $upd")
                }
            }

            return InMemoryWord2VecDB(terms, storage)
        } catch (oom: OutOfMemoryError) {
            throw IOException("Couldn't load word vectors in RAM: $input.", oom)
        }
    }
}


// This main file answers the question: What if we used word vectors to try and assign bigram weights?
// Conclusion: not possible with my unsupervised intuition.
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val inputPath = argp.get("vectors", "data/paragraphs.norm.fastText.vec")
    val db = loadWord2VecTextFormat(File(inputPath))
    println("Loaded ${db.N} vectors of dim ${db.dim}")

    val stopwords = computeMeanVector(inqueryStop.mapNotNull { db[it] }) ?: db.bgVector

    val dataset = DataPaths.get("robust")
    val queries = dataset.title_qs
    val qrels = dataset.qrels
    val info = NamedMeasures()
    val measure = getEvaluator("ap")

    dataset.getIreneIndex().use { index ->
        index.env.estimateStats = "min"
        val qtok = queries.mapValues { (_,qtext) -> index.tokenize(qtext) }
        val qTermVectors = qtok.values.flatMap { qterms -> qterms.map { Pair(it, db[it] ?: db.bgVector) } }.associate { it }

        //println("stopwords=${stopwords.copy()}")
        qTermVectors.forEach { term, vec ->
            println("$term: ${vec.cosineSimilarity(stopwords)}")
        }

        qtok.forEach { qid, qterms ->
            val judgments = qrels[qid] ?: QueryJudgments(qid, emptyMap())
            val vecs = qterms.map { qTermVectors[it]!! }
            val qMeanVec = computeMeanVector(vecs) ?: error("No vectors?")
            val baselineQ = SequentialDependenceModel(qterms)

            val treatmentQ = SequentialDependenceModel(qterms, makeScorer = { c ->
                if (c is TextExpr) {
                    // unigram weighting
                    DirQLExpr(c).weighted(1.0 - MathUtils.sigmoid(qMeanVec.cosineSimilarity(qTermVectors[c.text]!!)))
                } else if (c is OrderedWindowExpr || c is UnorderedWindowExpr) {
                    val mVec = computeMeanVector(c.children.mapNotNull { it as? TextExpr }.map { qTermVectors[it.text]!! })!!
                    DirQLExpr(c).weighted(1.0 - MathUtils.sigmoid(qMeanVec.cosineSimilarity(mVec)))
                } else error("No other node in SDM! But, actually found: $c")
            })

            println(simplify(treatmentQ))

            val results = index.pool(mapOf("baseline" to baselineQ, "treatment" to treatmentQ), 1000)

            val baseline = results["baseline"]!!.toQueryResults(index, qid)
            val treatment = results["treatment"]!!.toQueryResults(index, qid)

            info.push("ap1", measure.evaluate(baseline, judgments))
            info.push("ap2", measure.evaluate(treatment, judgments))

            println("$qid: $qterms $info")
        }
    }

    println(info)
}

object ExactWordProb {
    @JvmStatic fun main(args: Array<String>) {
        val argp = Parameters.parseArgs(args)
        val wordWordCounts = HashMap<String, HashMap<String, Int>>()
        val contextNorms = HashMap<String, Int>()

        val inputPath = argp.get("vectors", "q-context-counts.summed.tsv.gz")
        File(inputPath).smartDoLines(true, total=3833527L) { line ->
            val cols = line.split('\t')
            val context = cols[0]
            val word = cols[1]
            val count = cols[2].toInt()
            wordWordCounts.computeIfAbsent(context, {HashMap()}).incr(word, count)
            contextNorms.incr(context, count)
        }

        val dataset = DataPaths.get("robust")
        val queries = dataset.title_qs
        val qrels = dataset.qrels
        val info = NamedMeasures()
        val measure = getEvaluator("ap")

        dataset.getIreneIndex().use { index ->
            index.env.estimateStats = "min"
            val qtok = queries.mapValues { (_,qtext) -> index.tokenize(qtext) }
            qtok.forEach { qid, qterms ->
                val judgments = qrels[qid] ?: QueryJudgments(qid, emptyMap())

                val wordBiList = qterms + qterms.mapEachSeqPair { lhs, rhs -> "$lhs $rhs" }

                val candidateProbabilities = wordBiList.associate { ctx ->
                    val norm = contextNorms[ctx] ?: 0
                    val scored = (wordWordCounts[ctx] ?: emptyMap<String, Int>()).mapValues { (_,freq) ->
                        safeDiv(freq, norm)
                    }
                    Pair(ctx, scored)
                }
                val candidates = candidateProbabilities.values.flatMapTo(HashSet()) { it.keys }

                val heap = ScoringHeap<ScoredWord>(30)
                for (c in candidates) {
                    val likelihood = candidateProbabilities.entries.sumByDouble { (obsv, probs) ->
                        val bgProb = (0.5 / (contextNorms[obsv] ?: 1).toDouble())
                        Math.log(probs[c] ?: bgProb)
                    }
                    heap.offer(ScoredWord(likelihood.toFloat(), c))
                }

                val expTerms = heap.sorted
                println(expTerms.joinToString(separator = "\t"))
                val logSumExp = MathUtils.logSumExp(expTerms.map { it.score.toDouble() }.toDoubleArray())

                val normExpTerms = expTerms.associate { Pair(it.word, Math.exp(it.score.toDouble() - logSumExp)) }.normalize()
                println(normExpTerms.entries.joinToString(separator = "\t"))

                candidateProbabilities.forEach { obsv, probs ->
                    val norm = (contextNorms[obsv] ?: 1).toDouble()
                    val bgProb = 0.5 / norm
                    val score = candidates.associate { Pair(it, Math.log(probs[it] ?: bgProb)) }
                }

                val baselineQ = SequentialDependenceModel(qterms)
                val treatmentQ = SumExpr(baselineQ.weighted(0.7),
                        SumExpr(normExpTerms.map { DirQLExpr(TextExpr(it.key)).weighted(it.value) }).weighted(0.3))

                println(simplify(treatmentQ))

                val results = index.pool(mapOf("baseline" to baselineQ, "treatment" to treatmentQ), 1000)

                val baseline = results["baseline"]!!.toQueryResults(index, qid)
                val treatment = results["treatment"]!!.toQueryResults(index, qid)

                info.push("ap1", measure.evaluate(baseline, judgments))
                info.push("ap2", measure.evaluate(treatment, judgments))

                println("$qid: $qterms $info")
            }
        }

        println(info)
    }
}

object PredictIDFDataset {
    @JvmStatic fun main(args: Array<String>) {

        val argp = Parameters.parseArgs(args)
        val inputPath = argp.get("vectors", "data/paragraphs.norm.fastText.vec")
        val db = loadWord2VecTextFormat(File(inputPath))
        println("Loaded ${db.N} vectors of dim ${db.dim}")
        val dsName = argp.get("dataset", "robust")
        val dataset = DataPaths.get(dsName)
        val numSamples = 50000

        val msg = CountingDebouncer(numSamples * 2L)
        WikiSource().getIreneIndex().use { wiki ->
            val wikiTitles =     (0 until wiki.totalDocuments).sample(numSamples).mapNotNull { i ->
                val str = wiki.getField(i, "title")?.stringValue()
                msg.incr()?.let { upd ->
                    println("pulling titles $upd")
                }
                str
            }

            println("Drew ${wikiTitles.size} titles from ''wiki'' index.")

            val phrases = HashMap<Pair<String, String>, CountStats>()
            for (title in wikiTitles) {
                val terms = wiki.tokenize(title)
                terms.forEachSeqPair {lhs, rhs ->
                    // compute actual statistics
                    phrases.computeIfAbsent(Pair(lhs, rhs), {
                        wiki.getStats(OrderedWindowExpr(listOf(TextExpr(lhs), TextExpr(rhs))))
                    })
                }
                msg.incr()?.let { upd ->
                    println("computing stats $upd")
                }
            }

            val wvec = phrases.keys.flatMapTo(HashSet()) { listOf(it.first, it.second) }.associate { Pair(it, db[it] ?: db.bgVector) }

            File("idf_pred.libsvm.gz").smartPrint { printer ->
                phrases.forEach { (kv, stats) ->
                    val target = stats.binaryProbability()
                    val lhs = wvec[kv.first]!!
                    val rhs = wvec[kv.second]!!
                    val ftrs = lhs.concat(rhs).toList().mapIndexed { i, value -> Pair(i+1, value) }
                    printer.println(ftrs.joinToString(separator = " ", prefix = "$target ") { (fid,value) -> "$fid:$value" })
                }
            }
        }
    }
}