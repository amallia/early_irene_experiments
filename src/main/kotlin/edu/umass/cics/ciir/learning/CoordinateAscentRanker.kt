package edu.umass.cics.ciir.learning

import edu.umass.cics.ciir.chai.safeDiv
import edu.umass.cics.ciir.chai.shuffle
import java.util.*

/**
 * This class implements the linear ranking model known as Coordinate Ascent. It was proposed in this paper:
 *  D. Metzler and W.B. Croft. Linear feature-based models for information retrieval. Information Retrieval, 10(3): 257-274, 2007.
 * @author vdang, jfoley
 */
data class CoordinateAscentRanker(
        val dataset: MachineLearningInput,
        val nMaxIteration: Int = 25,
        val numRestarts: Int = 1,
        val stepBase: Double = 0.05,
        val stepScale: Double = 2.0,
        val tolerance: Double = 0.001,
        val maxPerformance: Double = Double.MAX_VALUE,
        val regularized: Boolean = false,
        val slack: Double = 0.001, //regularized parameter
        val rand: Random = Random()
) {
    val weight: MutableVector = SimpleDenseVector(dataset.numFeatures).apply {
        clearToRandom(rand)
    }
    val shuffledFeatures: ArrayList<Int>
        get() = (0 until weight.dim).toCollection(ArrayList<Int>()).shuffle()

    fun learn(): Vector {
        val state = CoordinateAscentState(this)

        var consecutiveFailures = 0
        while(true) {
            if (consecutiveFailures >= dataset.numFeatures) break

            //println("--Start New Shuffle-- ${state.bestModelScore} ${state.weight}")
            // Get a new set of shuffled features
            val features = shuffledFeatures
            for (fid in features) {
                // clear any caching, just in case.
                val startScore = state.score()
                val startWeight = state.weight[fid]
                var bestWeight = startWeight
                var success = false

                for (dir in listOf(-1, 0, 1)) {
                    var totalStep: Double
                    // restore original weight:
                    state.weight[fid] = startWeight
                    var step = 0.001 * dir
                    if (startWeight != 0.0 && Math.abs(step) > 0.5 * Math.abs(startWeight)) {
                        step = stepBase * Math.abs(startWeight)
                    }
                    totalStep = step
                    val numIter = if (dir == 0) {
                        totalStep =- startWeight
                        1
                    } else { nMaxIteration }

                    for (j in (0 until numIter)) {
                        val w = startWeight + totalStep
                        state.weight[fid] = w
                        val score = state.score(fid, totalStep)

                        if (score > state.bestModelScore) {
                            //println(" -> w[$fid]=${"%1.3f".format(w)}, score=${"%1.3f".format(score)}")
                            state.bestModelScore = score
                            bestWeight = w
                            success = true
                        }

                        step *= stepScale
                        totalStep += step
                    }
                    // Don't check multiple directions if one is fruitful.
                    if (success) break
                }
                // Was feature successful?
                state.weight[fid] = bestWeight
                state.bestModel.copyFrom(state.weight)
                if (success) {
                    consecutiveFailures = 0
                } else {
                    consecutiveFailures++
                }
            } // for all features.

            // If changes are minimal, exit.
            if (Math.abs(state.bestModelScore - state.startScore) < tolerance) {
                break
            }
        } // Shuffle features and improve until we can't anymore

        return state.bestModel
    }
}

class CoordinateAscentState(val params: CoordinateAscentRanker) {
    val weight: MutableVector = SimpleDenseVector(params.dataset.numFeatures).apply {
        clearToRandom(params.rand)
    }
    var bestModel = weight.copy()
    var bestModelScore = 0.0;
    val predictions = DoubleArray(params.dataset.numInstances)
    val predicted: ArrayList<Predicted> = (0 until predictions.size).mapTo(ArrayList()) { Predicted(it, predictions, params.dataset) }
    val startScore = score()
    val numRelevant = predicted.count { it.correct }

    fun score(fidChanged: Int? = null, amtChanged: Double? = null): Double {
        if (fidChanged != null) {
            val delta = amtChanged ?: 0.0
            for (i in (0 until params.dataset.numInstances)) {
                predictions[i] += delta * params.dataset[i][fidChanged]
            }
        } else {
            for (i in (0 until params.dataset.numInstances)) {
                predictions[i] = weight.dotp(params.dataset[i])
            }
        }
        return computeAP(predicted, numRelevant)
    }

    class Predicted(val index: Int, val predictions: DoubleArray, val dataset: MachineLearningInput) : Prediction {
        override val score: Double
            get() = predictions[index]
        override val correct: Boolean
            get() = dataset.truth(index)
    }
}


data class SimplePrediction(override val score: Double, override val correct: Boolean): Prediction { }
interface Prediction : Comparable<Prediction> {
    val score: Double
    val correct: Boolean
    override fun compareTo(other: Prediction): Int {
        // High before low.
        val cmp = -java.lang.Double.compare(score, other.score)
        if (cmp != 0) return cmp
        // False before true.
        return correct.compareTo(other.correct)
    }
}

// If this appears terrible, remember to sort!
fun computeAP(input: List<Prediction>, numRelevant: Int): Double {
    if (input.isEmpty() || numRelevant == 0) return 0.0

    var sumPrecision = 0.0
    var recallPointCount = 0
    input.sorted().forEachIndexed { i, item ->
        if (item.correct) {
            val rank = i + 1
            recallPointCount++
            sumPrecision += safeDiv(recallPointCount, rank)
        }
    }

    return sumPrecision / numRelevant.toDouble()
}
