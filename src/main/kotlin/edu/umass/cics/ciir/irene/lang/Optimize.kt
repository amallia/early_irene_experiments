package edu.umass.cics.ciir.irene.lang

import edu.umass.cics.ciir.iltr.RREnv
import edu.umass.cics.ciir.irene.DataNeeded
import edu.umass.cics.ciir.irene.simplifyBooleanExpr
import edu.umass.cics.ciir.sprf.incr

/**
 *
 * @author jfoley.
 */
fun simplify(q: QExpr): QExpr {
    val pq = q.copy()

    // combine weights and boolean nodes until query stops changing.
    var changed = true
    while(changed) {
        changed = false
        while (combineWeights(pq)) {
            changed = true
        }
        while (simplifyBooleanExpr(pq)) {
            changed = true
        }
    }

    return pq
}

fun computeQExprStats(index: RREnv, root: QExpr) {
    root.visit { q ->
        when (q) {
            is TextExpr -> {
                q.stats = index.getStats(q.text, q.statsField())
            }
            is LengthsExpr -> {
                q.stats = index.fieldStats(q.statsField!!)
            }
            else -> { }
        }
    }
}

fun combineWeights(q: QExpr): Boolean {
    var changed = false
    when(q) {
    // Flatten nested-weights.
        is WeightExpr -> {
            val c = q.child
            if (c is WeightExpr) {
                q.child = c.child
                q.weight *= c.weight
                changed = true
                //println("weight(weight(x)) -> weight(x)")
            }
        }
    // Pull weights up into CombineExpr.
        is CombineExpr -> {
            val newChildren = arrayListOf<QExpr>()
            val newWeights = arrayListOf<Double>()
            q.entries.forEach { (c, w) ->
                if (c is CombineExpr) {
                    // flatten combine(combine(...))
                    c.children.zip(c.weights).forEach { (cc, cw) ->
                        newChildren.add(cc)
                        newWeights.add(w*cw)
                    }
                    changed = true
                } else if (c is WeightExpr) {
                    //println("combine(...weight(x)..) -> combine(...x...)")
                    newChildren.add(c.child)
                    newWeights.add(c.weight * w)
                    changed = true
                } else {
                    newChildren.add(c)
                    newWeights.add(w)
                }
                q.children = newChildren
                q.weights = newWeights
            }
        }
        else -> {}
    }

    // Statically-combine any now-redundant-children in CombineExpr:
    if (q is CombineExpr && q.children.toSet().size < q.children.size) {
        val weights = HashMap<QExpr, Double>()
        q.entries.forEach { (child, weight) ->
            weights.incr(child, weight)
        }

        val newChildren = arrayListOf<QExpr>()
        val newWeights = arrayListOf<Double>()
        weights.forEach { c,w ->
            newChildren.add(c)
            newWeights.add(w)
        }
        q.children = newChildren
        q.weights = newWeights
        changed = true
    }

    q.children.forEach {
        changed = changed || combineWeights(it)
    }
    return changed
}

class TypeCheckError(msg: String): Exception(msg)

fun analyzeDataNeededRecursive(q: QExpr, needed: DataNeeded= DataNeeded.DOCS) {
    var childNeeds = needed
    childNeeds = when(q) {
        is TextExpr -> {
            if (childNeeds == DataNeeded.SCORES) {
                throw TypeCheckError("Cannot convert q=$q to score. Try DirQLExpr(q) or BM25Expr(q)")
            }
            q.needed = childNeeds
            childNeeds
        }
        is LengthsExpr -> return
        is AndExpr, is OrExpr -> DataNeeded.DOCS
    // Pass through whatever at this point.
        is WhitelistMatchExpr, is AlwaysMatchExpr, is NeverMatchExpr, is MultiExpr -> childNeeds
        is LuceneExpr, is SynonymExpr -> childNeeds
        is WeightExpr, is CombineExpr, is MultExpr, is MaxExpr -> {
            DataNeeded.SCORES
        }
        is UnorderedWindowCeilingExpr, is SmallerCountExpr -> {
            if (q.children.size <= 1) {
                throw TypeCheckError("Need more than 1 child for an count summary Expr, e.g. $q")
            }
            DataNeeded.COUNTS
        }
        is UnorderedWindowExpr, is OrderedWindowExpr -> {
            if (q.children.size <= 1) {
                throw TypeCheckError("Need more than 1 child for an window Expr, e.g. $q")
            }
            DataNeeded.POSITIONS
        }
        is AbsoluteDiscountingQLExpr, is BM25Expr, is DirQLExpr ->  DataNeeded.COUNTS
        is CountToScoreExpr ->  DataNeeded.COUNTS
        is BoolToScoreExpr -> DataNeeded.DOCS
        is CountToBoolExpr -> DataNeeded.COUNTS
        is RequireExpr -> {
            analyzeDataNeededRecursive(q.cond, DataNeeded.DOCS)
            analyzeDataNeededRecursive(q.value, childNeeds)
            return
        }
        is ConstScoreExpr -> return assert(needed == DataNeeded.SCORES)
        is ConstCountExpr -> return assert(needed == DataNeeded.COUNTS || needed == DataNeeded.DOCS)
        is ConstBoolExpr -> return assert(needed == DataNeeded.DOCS)
    }
    q.children.forEach { analyzeDataNeededRecursive(it, childNeeds) }
}

fun applyEnvironment(env: RREnv, root: QExpr) {
    root.visit { q ->
        when(q) {
            is LuceneExpr -> q.parse(env as? IreneQueryLanguage ?: error("LuceneExpr in environment without LuceneParser."))
            is DirQLExpr -> if (q.mu == null) {
                q.mu = env.defaultDirichletMu
            }
            is AbsoluteDiscountingQLExpr -> if (q.delta == null) {
                q.delta = env.absoluteDiscountingDelta
            }
            is BM25Expr -> {
                if (q.b == null) q.b = env.defaultBM25b
                if (q.k == null) q.k = env.defaultBM25k
            }
            else -> q.applyEnvironment(env)
        }
    }
}

