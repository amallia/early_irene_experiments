package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.CountStatsStrategy
import edu.umass.cics.ciir.irene.LazyCountStats
import edu.umass.cics.ciir.irene.MinEstimatedCountStats
import edu.umass.cics.ciir.irene.ProbEstimatedCountStats
import edu.umass.cics.ciir.irene.lang.*
import gnu.trove.set.hash.TIntHashSet
import org.apache.lucene.index.Term

/**
 * This function translates the public-facing query language ([QExpr] and subclasses) to a set of private-facing operators ([QueryEvalNode] and subclasses).
 * @author jfoley.
 */
fun exprToEval(q: QExpr, ctx: IQContext): QueryEvalNode = when(q) {
    is TextExpr -> ctx.create(Term(q.countsField(), q.text), q.needed, q.stats ?: error("Missed computeQExprStats pass."))
    is LuceneExpr -> TODO()
    is SynonymExpr -> TODO()
    is AndExpr -> BooleanAndEval(q.children.map { exprToEval(it, ctx) })
    is OrExpr -> BooleanOrEval(q.children.map { exprToEval(it, ctx) })
    is CombineExpr -> WeightedSumEval(
            q.children.map { exprToEval(it, ctx) },
            q.weights.map { it }.toDoubleArray())
    is MultExpr -> TODO()
    is MaxExpr -> MaxEval(q.children.map { exprToEval(it, ctx) })
    is WeightExpr -> WeightedEval(exprToEval(q.child, ctx), q.weight.toFloat())
    is DirQLExpr -> DirichletSmoothingEval(exprToEval(q.child, ctx) as CountEvalNode, q.mu!!)
    is BM25Expr -> BM25ScoringEval(exprToEval(q.child, ctx) as CountEvalNode, q.b!!, q.k!!)
    is CountToScoreExpr -> TODO()
    is BoolToScoreExpr -> TODO()
    is CountToBoolExpr -> TODO()
    is RequireExpr -> RequireEval(exprToEval(q.cond, ctx), exprToEval(q.value, ctx))
    is OrderedWindowExpr -> OrderedWindow(
            computeCountStats(q, ctx),
            q.children.map { exprToEval(it, ctx) as PositionsEvalNode }, q.step)
    is UnorderedWindowExpr -> UnorderedWindow(
            computeCountStats(q, ctx),
            q.children.map { exprToEval(it, ctx) as PositionsEvalNode }, q.width)
    is SmallerCountExpr -> SmallerCountWindow(
            computeCountStats(q, ctx),
            q.children.map { exprToEval(it, ctx) as CountEvalNode })
    is UnorderedWindowCeilingExpr -> UnorderedWindowCeiling(
            computeCountStats(q, ctx),
            q.width,
            q.children.map { exprToEval(it, ctx) as CountEvalNode })
    is ConstScoreExpr -> ConstEvalNode(q.x.toFloat())
    is ConstCountExpr -> ConstCountEvalNode(q.x, exprToEval(q.lengths, ctx) as CountEvalNode)
    is ConstBoolExpr -> if(q.x) ConstTrueNode(ctx.numDocs()) else ConstEvalNode(0)
    is AbsoluteDiscountingQLExpr -> error("No efficient method to implement AbsoluteDiscountingQLExpr in Irene backend; needs numUniqWords per document.")
    is MultiExpr -> MultiEvalNode(q.children.map { exprToEval(it, ctx) }, q.names)
    is LengthsExpr -> ctx.createLengths(q.statsField!!, q.stats!!)
    is NeverMatchExpr -> FixedMatchEvalNode(false, exprToEval(q.trySingleChild, ctx))
    is AlwaysMatchExpr -> FixedMatchEvalNode(true, exprToEval(q.trySingleChild, ctx))
    is WhitelistMatchExpr -> WhitelistMatchEvalNode(TIntHashSet(ctx.selectRelativeDocIds(q.docIdentifiers!!)))
    is ProxExpr -> TODO()
}

fun approxStats(q: QExpr, method: String): CountStatsStrategy {
    if (q is OrderedWindowExpr || q is UnorderedWindowExpr || q is SmallerCountExpr || q is UnorderedWindowCeilingExpr) {
        val cstats = q.children.map { c ->
            if (c is TextExpr) {
                c.stats!!
            } else if (c is ConstCountExpr) {
                c.lengths.stats!!
            } else if (c is LengthsExpr) {
                c.stats!!
            } else {
                error("Can't estimate stats with non-TextExpr children. $c")
            }
        }
        return when(method) {
            "min" -> MinEstimatedCountStats(q.copy(), cstats)
            "prob" -> ProbEstimatedCountStats(q.copy(), cstats)
            else -> TODO("estimateStats strategy = $method")
        }
    } else {
        TODO("approxStats($q)")
    }
}

fun computeCountStats(q: QExpr, ctx: IQContext): CountStatsStrategy {
    if (q is OrderedWindowExpr || q is UnorderedWindowExpr || q is SmallerCountExpr || q is UnorderedWindowCeilingExpr) {
        val method = ctx.env.estimateStats ?: return LazyCountStats(q.copy(), ctx.env)
        return approxStats(q, method)
    } else {
        TODO("computeCountStats($q)")
    }
}

