package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.Debouncer
import edu.umass.cics.ciir.sample
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.NamedMeasures
import edu.umass.cics.ciir.sprf.getEvaluators
import edu.umass.cics.ciir.sprf.mean
import gnu.trove.list.array.TDoubleArrayList
import org.lemurproject.galago.utility.Parameters
import java.util.*

/**
 * @author jfoley
 */

fun ReflexivePair(lhs: Int, rhs: Int) = Pair(minOf(lhs, rhs), maxOf(lhs, rhs))

fun createNewVector(n: Int=300, d: Int=30, rand: Random=Random()): FloatArray {
    val pieces = floatArrayOf(-1f, 1f)
    val out = FloatArray(n, {0f})
    (0 until d).forEach {
        out[rand.nextInt(n)] = pieces[rand.nextInt(pieces.size)]
    }
    return out
}
fun FloatArray.clear() {
    indices.forEach { i ->
        this[i] = 0f
    }
}
fun FloatArray.normalize() {
    val total = this.map { it * it }.sum()
    if (total > 0f) {
        indices.forEach { i ->
            this[i] /= total
        }
    }
}

inline fun <T> List<T>.forEachWindow(width: Int=5, fn: (List<T>)->Unit) {
    (0 until this.size - width).forEach { i ->
        fn(this.subList(i, i+width))
    }
}

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val dsName = argp.get("dataset", "robust")
    val dataset = DataPaths.get(dsName)
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()
    val qrels = dataset.getQueryJudgments()
    val fbTerms = 100
    val rmLambda = 0.2
    val passageSize = 50
    val width = 5
    val dim = 30
    val dimSet = 3
    val random = Random()

    dataset.getIndex().use { retr ->
        val env = RREnv(retr)
        forEachQuery(dsName) { q ->
            if (q.qid != "301") return@forEachQuery
            val queryJudgments = qrels[q.qid]
            println("${q.qid} ${q.qterms}")

            val vocab = HashMap<String, Int>()
            val vectors = HashMap<Int, FloatArray>()
            val passages = q.docs.mapIndexed { i, doc ->
                val intTerms = doc.terms.map { w -> vocab.computeIfAbsent(w, {vocab.size}) }
                (0 until intTerms.size step passageSize).map { start ->
                    val end = minOf(start+50, doc.terms.size)
                    Pair(i, intTerms.subList(start, end))
                }
            }.flatten()
            println("numPassages = ${passages.size}, vocab = ${vocab.size}")

            vocab.values.forEach { vectors.put(it, createNewVector(dim, dimSet)) }

            val context = FloatArray(dim) { 0f }
            val msg = Debouncer()
            passages.forEach { (_, pws) ->
                pws.forEachWindow(width) { ws ->
                    // clear context:
                    context.clear()
                    // sum up context:
                    ws.forEach { w ->
                        val wv = vectors[w]!!
                        context.indices.forEach { i ->
                            context[i] += wv[i]
                        }
                    }
                    if (msg.ready()) {
                        println(ws.map{ w -> vectors[w]!!.toList() })
                        println(context.toList())
                    }
                    ws.forEach { w ->
                        val wv = vectors[w]!!
                        context.indices.forEach { i ->
                            wv[i] += context[i]
                        }
                        wv.normalize()
                    }
                }
            }

            vectors.entries.sample(5).forEach { (wi, wvi) ->
                println("\tw[$wi] = ${wvi.toList()}")
            }

            val passageVecs = HashMap<Int, ArrayList<FloatArray>>()
            passages.forEach { (docid, passage) ->
                val pvec = FloatArray(dim)
                passage.map {
                    val wv = vectors[it]!!
                    wv.indices.forEach { i ->
                        pvec[i] += wv[i]
                    }
                }
                pvec.indices.forEach { i ->
                    pvec[i] /= passage.size.toFloat()
                }

                passageVecs.computeIfAbsent(docid, {ArrayList<FloatArray>()}).add(pvec)
            }


            val similarities = HashMap<Pair<Int,Int>, Double>()
            val dvsim = HashMap<Pair<Int,Int>, Double>()
            val docVectors = q.docs.map { it.freqs }

            // All pairs:
            (0 until docVectors.size-1).forEach { i ->
                (i until docVectors.size).forEach { j ->
                    val key = ReflexivePair(i, j)
                    similarities.put(key,
                            cosineSimilarity(docVectors[i], docVectors[j]))
                    dvsim.put(key, meanSimilarity(passageVecs[i]!!, passageVecs[j]!!))
                }
            }

            println(similarities.size)
            val keys = similarities.keys.sample(30)
            println(keys.map { similarities[it] })
            println(keys.map { dvsim[it] })
        }
    }
}

fun meanSimilarity(lhs: List<FloatArray>, rhs: List<FloatArray>): Double {
    val simv = TDoubleArrayList()
    lhs.forEach { lvec ->
        rhs.forEach { rvec ->
            simv.add(dotP(lvec, rvec))
        }
    }
    return simv.mean()
}

fun dotP(x: FloatArray, y: FloatArray): Double {
    var sum = 0.0
    x.indices.forEach { i ->
        sum += x[i] * y[i]
    }
    return sum
}

fun cosineSimilarity(lhs: BagOfWords, rhs: BagOfWords): Double {
    var num = 0.0
    lhs.counts.forEachEntry { t, c ->
        val c2 = rhs.count(t)
        num += c * c2
        true
    }
    return num / (lhs.l2norm * rhs.l2norm)
}

