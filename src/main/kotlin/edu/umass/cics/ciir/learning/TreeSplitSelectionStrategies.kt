package edu.umass.cics.ciir.learning

/**
 *
 * @author jfoley.
 */

fun getTreeSplitSelectionStrategy(importanceStrategyName: String): TreeSplitSelectionStrategy = when(importanceStrategyName) {
    "meanDiff" -> DifferenceInLabelMeans()
    "minStdDev" -> MinLabelStdDeviation()
    "gini" -> BinaryGiniImpurity()
    "entropy" -> InformationGain()
    "variance" -> TrueVarianceReduction()
    "conf-variance" -> ConfidentVarianceReduction()
    else -> TODO(importanceStrategyName)
}

interface TreeSplitSelectionStrategy {
    fun importance(fsc: FeatureSplitCandidate): Double
}

class DifferenceInLabelMeans : TreeSplitSelectionStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        return Math.abs(fsc.rhs.labelStats.mean - fsc.lhs.labelStats.mean)
    }
}
// The best split is the one where the standard deviation of labels goes very low.
// Ideally in both splits, here just in one is enough (we'll split the other again.
class MinLabelStdDeviation : TreeSplitSelectionStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        return -minOf(fsc.rhs.labelStats.standardDeviation, fsc.lhs.labelStats.standardDeviation)
    }
}
class MultLabelStdDeviation : TreeSplitSelectionStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        return -fsc.rhs.labelStats.standardDeviation * fsc.lhs.labelStats.standardDeviation
    }
}
// Typically want to minimize impurity, so negative.
class BinaryGiniImpurity : TreeSplitSelectionStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        val lhs_size = fsc.lhs.size.toDouble()
        val rhs_size = fsc.rhs.size.toDouble()
        val size = lhs_size + rhs_size
        return -(fsc.lhs.giniImpurity()*lhs_size + fsc.lhs.giniImpurity()*rhs_size) / size
    }
}
// Typically want to minimize impurity, so negative.
class InformationGain : TreeSplitSelectionStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        val lhs_size = fsc.lhs.size.toDouble()
        val rhs_size = fsc.rhs.size.toDouble()
        val size = lhs_size + rhs_size
        // entropy(parent) is a constant under comparison
        return -(fsc.lhs.entropy()*lhs_size + fsc.lhs.entropy()*rhs_size) / size
    }
}
// Variance reduction
class TrueVarianceReduction : TreeSplitSelectionStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        val lhs_size = fsc.lhs.size.toDouble()
        val rhs_size = fsc.rhs.size.toDouble()
        val size = lhs_size + rhs_size
        // variance(parent) is a constant under comparison
        return -(fsc.lhs.labelStats.variance*lhs_size + fsc.lhs.labelStats.variance*rhs_size) / size
    }
}

// Variance reduction
class ConfidentVarianceReduction : TreeSplitSelectionStrategy {
    override fun importance(fsc: FeatureSplitCandidate): Double {
        val lhs_size = fsc.lhs.size.toDouble()
        val rhs_size = fsc.rhs.size.toDouble()
        val size = lhs_size + rhs_size
        // variance(parent) is a constant under comparison
        return -(fsc.lhs.labelStats.variance*fsc.lhs.confidence*lhs_size + fsc.lhs.labelStats.variance*fsc.rhs.confidence*rhs_size) / size
    }
}
