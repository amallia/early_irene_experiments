package edu.umass.cics.ciir.irene

/**
 * @author jfoley
 */
fun createOptimizedMovementExpr(q: QExpr): QExpr = when(q) {
    // OR nodes:
    is SynonymExpr, is OrExpr, is CombineExpr, is MultExpr, is MaxExpr, is MultiExpr -> OrExpr(q.children.map { createOptimizedMovementExpr(it) })

    // Leaves:
    is TextExpr, is LuceneExpr, is LengthsExpr, is ConstCountExpr, is ConstBoolExpr, is ConstScoreExpr -> q.copy()

    // AND nodes:
    is AndExpr, is MinCountExpr, is OrderedWindowExpr, is UnorderedWindowExpr -> AndExpr(q.children.map { createOptimizedMovementExpr(it) })

    // Transformers are
    is CountToScoreExpr, is BoolToScoreExpr, is CountToBoolExpr, is AbsoluteDiscountingQLExpr, is BM25Expr, is WeightExpr, is DirQLExpr -> createOptimizedMovementExpr(q.trySingleChild)

    // NOTE: Galago semantics, only look at cond. This is not an AND like you might think.
    is RequireExpr -> createOptimizedMovementExpr(q.cond)
}

fun simplifyBooleans(q: QExpr): QExpr {
    val pq = q.copy()
    // combine weights until query stops changing.
    while(simplifyBooleanExpr(pq)) { }
    return pq
}

fun simplifyBooleanExpr(q: QExpr): Boolean {
    var changed = false

    // Flatten nesting: X(X(a b) c) -> X(a b c) when X is AND or OR
    when(q) {
        is AndExpr -> {
            q.children =  q.children.flatMap { c ->
                if (c is AndExpr) {
                    changed = true
                    c.children
                } else listOf(c)
            }
        }
        is OrExpr -> {
            q.children = q.children.flatMap { c ->
                if (c is OrExpr) {
                    changed = true
                    c.children
                } else listOf(c)
            }
        }
        else -> {}
    }

    // And remove duplicates: X(a b c a a) -> X(a b c) for AND and OR.
    // (and a few other operators)
    when (q) {
        is AndExpr, is OrExpr, is SynonymExpr, is MaxExpr, is MinCountExpr -> {
            val uniq= q.children.toSet()
            if (uniq.size < q.children.size) {
                changed = true
                (q as OpExpr).children = uniq.toList()
            }
        }
        // TODO, someday MultiExpr?
        else ->{}
    }

    // Find any OR(X ... AND(Y) ) where X subsets Y, and replace with OR(X)
    // This is a specialized optimization for term dependency models, e.g., SDM and FDM, where you have unigrams (a b c) and bigrams (a b) and (b c).
    // This is the expression OR(a b c AND(a b) AND (b c)) but can be computed cheaper as OR(a b c)
    // a + a'b -> a
    // This will especially help MultiExpr pooling situations.
    // This is the implication of rules from boolean algebra:
    // A + AB -> A(1+B) -> A
    // Ergo, the rule is actually more broad than the SDM case: if there exists a term in an AND child of an OR that is also a part of that or, it can be un-distributed out, and the rest of the term cancelled.
    if (q is OrExpr) {
        val childAnds = ArrayList<AndExpr>()
        q.children.forEach {
            if (it is AndExpr) {
                childAnds.add(it)
            }
        }
        if (childAnds.isNotEmpty()) {
            val provablyRedundant = childAnds.mapNotNull { andExpr ->
                val topOrChildren = q.children.filterNot { (it === andExpr) }
                val reduce = andExpr.children.any { topOrChildren.contains(it) }
                // Now if we've found a term in a child AND that proves it redundant:
                if (reduce) {
                    andExpr
                } else {
                    null
                }
            }

            if (provablyRedundant.isNotEmpty()) {
                changed = true
                q.children = q.children.filterNot { provablyRedundant.contains(it) }
            }
        }
    }

    q.children.forEach {
        changed = changed || combineWeights(it)
    }
    return changed
}
