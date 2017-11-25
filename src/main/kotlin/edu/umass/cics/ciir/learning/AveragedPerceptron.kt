package edu.umass.cics.ciir.learning

import edu.umass.cics.ciir.chai.safeDiv

/**
 *
 * @author jfoley.
 */
data class PerceptronResult(val converged: Boolean, val accuracy: Double, val pos: Int, val neg: Int, val weights: MutableVector) {
    init {
        weights.normalizeL2()
    }
    val informative: Boolean get() = pos != 0 && neg != 0
}

fun learnAveragePerceptron(data: MachineLearningInput, maxIters: Int=100): PerceptronResult {
    val N = data.numInstances
    val D = data.numFeatures
    data.shuffle()

    // average perceptron
    val tmpW = SimpleDenseVector(D)
    tmpW.clearToRandom()
    val w = SimpleDenseVector(D)
    var wLife = 0
    var correct = 0
    var pos = 0
    var neg = 0
    var changed = false

    for (iter in (0 until maxIters)) {
        correct = 0
        pos = 0
        neg = 0
        changed = false
        for (i in (0 until N)) {
            val fv = data[i]
            val label = data.label(i)
            val pred = if (fv.dotp(w) >= 0.0) 1 else -1

            if (pred > 0) { pos++ } else { neg++ }

            if (pred != label) {
                if (wLife > 0) {
                    w.incr(wLife.toDouble(), tmpW)
                }

                // update
                tmpW.incr(label.toDouble(), fv)

                changed = true
                wLife++
            } else {
                wLife++
                correct++
            }
        }

        // exit early if possible.
        if (!changed && correct == N) {
            // final averaged update
            w.incr(wLife.toDouble(), tmpW)
            return PerceptronResult(true, 1.0, pos, neg, w)
        }
    }
    // final averaged update
    w.incr(wLife.toDouble(), tmpW)

    return PerceptronResult(false, safeDiv(correct, N), pos, neg, w)
}
