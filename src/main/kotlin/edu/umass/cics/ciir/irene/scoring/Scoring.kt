package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.*
import org.apache.lucene.index.Term
import org.apache.lucene.search.DocIdSetIterator
import org.apache.lucene.search.Explanation
import java.io.File

/**
 * This class translates the public-facing query language (QExpr and subclasses) to a set of private-facing operators (QueryEvalNode and subclasses).
 * @author jfoley.
 */

fun exprToEval(q: QExpr, ctx: IQContext): QueryEvalNode = when(q) {
    is TextExpr -> ctx.create(Term(q.field, q.text), q.needed, q.stats ?: error("Missed applyIndex pass."))
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
    is BM25Expr -> TODO()
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
    is ConstScoreExpr -> ConstEvalNode(q.x.toFloat())
    is ConstCountExpr -> ConstEvalNode(q.x)
    is ConstBoolExpr -> if(q.x) ConstTrueNode(ctx.numDocs()) else ConstEvalNode(0)
}

fun approxStats(q: QExpr, method: String): CountStatsStrategy {
    if (q is OrderedWindowExpr || q is UnorderedWindowExpr) {
        val cstats = q.children.map { c ->
            if (c is TextExpr) {
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
    if (q is OrderedWindowExpr || q is UnorderedWindowExpr) {
        val method = ctx.env.estimateStats ?: return LazyCountStats(q.copy(), ctx.index)
        return approxStats(q, method)
    } else {
        TODO("computeCountStats($q)")
    }
}

const val NO_MORE_DOCS = DocIdSetIterator.NO_MORE_DOCS
interface QueryEvalNode {
    fun docID(): Int
    // Return a score for a document.
    fun score(doc: Int): Float
    // Return an count for a document.
    fun count(doc: Int): Int
    // Return an boolean for a document. (must be true if you want this to be ranked).
    fun matches(doc: Int): Boolean
    // Return an explanation for a document.
    fun explain(doc: Int): Explanation

    // Used to accelerate AND and OR matching if accurate.
    fun estimateDF(): Long
    // Move forward to (docID() >= target) don't bother checking for match.
    fun advance(target: Int): Int

    // The following movements should never be overridden.
    // They may be used as convenient to express other operators, i.e. call nextMatching on your children instead of advance().

    // Use advance to move forward until match.
    fun nextMatching(doc: Int): Int {
        var id = maxOf(doc, docID());
        while(id < NO_MORE_DOCS) {
            if (matches(id)) {
                assert(docID() == id)
                return id
            }
            id = advance(id+1)
        }
        return id;
    }
    // Step to the next matching document.
    fun nextDoc(): Int = nextMatching(docID()+1)
    // True if iterator is exhausted.
    val done: Boolean get() = docID() == NO_MORE_DOCS
    // Safe interface to advance. Only moves forward if need be.
    fun syncTo(target: Int) {
        if (docID() < target) {
            advance(target)
            assert(docID() >= target)
        }
    }
}
interface CountEvalNode : QueryEvalNode {
    override fun score(doc: Int) = count(doc).toFloat()
    fun getCountStats(): CountStats
    fun length(doc: Int): Int
}
interface PositionsEvalNode : CountEvalNode {
    fun positions(doc: Int): PositionsIter
}

class ConstTrueNode(val numDocs: Int) : QueryEvalNode {
    var current = 0
    override fun docID(): Int = current
    override fun score(doc: Int): Float = 1f
    override fun count(doc: Int): Int = 1
    override fun matches(doc: Int): Boolean = true
    override fun explain(doc: Int): Explanation = Explanation.match(1f, "ConstTrueNode")
    override fun estimateDF(): Long = numDocs.toLong()
    override fun advance(target: Int): Int {
        if (current < target) {
            current = target
            if (current >= numDocs) {
                current = NO_MORE_DOCS
            }
        }
        return current
    }

}

class ConstEvalNode(val count: Int, val score: Float) : QueryEvalNode {
    constructor(count: Int) : this(count, count.toFloat())
    constructor(score: Float) : this(1, score)

    override fun docID(): Int = NO_MORE_DOCS
    override fun score(doc: Int): Float = score
    override fun count(doc: Int): Int = count
    override fun matches(doc: Int): Boolean = false

    override fun explain(doc: Int): Explanation = Explanation.noMatch("ConstEvalNode(count=$count, score=$score)")
    override fun estimateDF(): Long = 0L
    override fun advance(target: Int): Int = NO_MORE_DOCS
}

private class RequireEval(val cond: QueryEvalNode, val score: QueryEvalNode, val miss: Float=-Float.MAX_VALUE): QueryEvalNode {
    override fun score(doc: Int): Float = if (cond.matches(doc)) { score.score(doc) } else miss
    override fun count(doc: Int): Int = if (cond.matches(doc)) { score.count(doc) } else 0
    override fun matches(doc: Int): Boolean = cond.matches(doc) && score.matches(doc)
    override fun explain(doc: Int): Explanation {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
    override fun estimateDF(): Long = minOf(score.estimateDF(), cond.estimateDF())
    override fun docID(): Int = cond.docID()
    override fun advance(target: Int): Int = cond.advance(target)
}

abstract class RecursiveEval<out T : QueryEvalNode>(val children: List<T>) : QueryEvalNode {
    val className = this.javaClass.simpleName
    val N = children.size
    override fun explain(doc: Int): Explanation {
        val expls = children.map { it.explain(doc) }
        if (matches(doc)) {
            return Explanation.match(score(doc), "$className.Match", expls)
        }
        return Explanation.noMatch("$className.Miss", expls)
    }
}
abstract class OrEval<out T : QueryEvalNode>(children: List<T>) : RecursiveEval<T>(children) {
    var current = children.map { it.nextMatching(0) }.min()!!
    val cost = children.map { it.estimateDF() }.max() ?: 0L
    val moveChildren = children.sortedByDescending { it.estimateDF() }
    override fun docID(): Int = current
    override fun advance(target: Int): Int {
        var newMin = NO_MORE_DOCS
        for (child in moveChildren) {
            var where = child.docID()
            if (where < target) {
                where = child.nextMatching(target)
            }
            assert(where >= target)
            newMin = minOf(newMin, where)
        }
        current = newMin
        return current
    }
    override fun estimateDF(): Long = cost

    override fun matches(doc: Int): Boolean {
        syncTo(doc)
        assert(docID() >= doc)
        return children.any { it.matches(doc) }
    }
}

abstract class AndEval<out T : QueryEvalNode>(children: List<T>) : RecursiveEval<T>(children) {
    private var current: Int = 0
    val cost = children.map { it.estimateDF() }.min() ?: 0L
    val moveChildren = children.sortedBy { it.estimateDF() }
    init {
        advanceToMatch()
    }
    override fun docID(): Int = current
    fun advanceToMatch(): Int {
        while(current < NO_MORE_DOCS) {
            var match = true
            for (child in moveChildren) {
                var pos = child.docID()
                if (pos < current) {
                    pos = child.nextMatching(current)
                }
                if (pos > current) {
                    current = pos
                    match = false
                    break
                }
            }

            if (match) return current
        }
        return current
    }

    override fun advance(target: Int): Int {
        if (current < target) {
            current = target
            return advanceToMatch()
        }
        return current
    }
    override fun estimateDF(): Long = cost

    override fun matches(doc: Int): Boolean {
        if (current > doc) return false
        if (current == doc) return true
        return advance(doc) == doc
    }
}

private class BooleanOrEval(children: List<QueryEvalNode>): OrEval<QueryEvalNode>(children) {
    override fun score(doc: Int): Float = if (matches(doc)) { 1f } else { 0f }
    override fun count(doc: Int): Int = if (matches(doc)) { 1 } else { 0 }
}
private class BooleanAndEval(children: List<QueryEvalNode>): AndEval<QueryEvalNode>(children) {
    override fun score(doc: Int): Float = if (matches(doc)) { 1f } else { 0f }
    override fun count(doc: Int): Int = if (matches(doc)) { 1 } else { 0 }
}

private class MaxEval(children: List<QueryEvalNode>) : OrEval<QueryEvalNode>(children) {
    override fun score(doc: Int): Float {
        var sum = 0f
        children.forEach {
            sum = maxOf(sum, it.score(doc))
        }
        return sum
    }
    override fun count(doc: Int): Int {
        var sum = 0
        children.forEach {
            sum = maxOf(sum, it.count(doc))
        }
        return sum
    }
}
// Also known as #combine for you galago/indri folks.
private class WeightedSumEval(children: List<QueryEvalNode>, val weights: DoubleArray) : OrEval<QueryEvalNode>(children) {
    override fun score(doc: Int): Float {
        var sum = 0.0
        children.forEachIndexed { i, child ->
            child.syncTo(doc)
            sum += weights[i] * child.score(doc)
        }
        return sum.toFloat()
    }

    override fun explain(doc: Int): Explanation {
        val expls = children.map { it.explain(doc) }
        if (matches(doc)) {
            return Explanation.match(score(doc), "$className.Match ${weights.toList()}", expls)
        }
        return Explanation.noMatch("$className.Miss ${weights.toList()}", expls)
    }

    override fun count(doc: Int): Int = error("Calling counts on WeightedSumEval is nonsense.")
    init { assert(weights.size == children.size, {"Weights provided to WeightedSumEval must exist for all children."}) }
}

private abstract class SingleChildEval<out T : QueryEvalNode> : QueryEvalNode {
    abstract val child: T
    override fun docID(): Int = child.docID()
    override fun advance(target: Int): Int = child.advance(target)
    override fun estimateDF(): Long = child.estimateDF()
    override fun matches(doc: Int): Boolean {
        child.syncTo(doc)
        return child.matches(doc)
    }
}

private class WeightedEval(override val child: QueryEvalNode, val weight: Float): SingleChildEval<QueryEvalNode>() {
    override fun score(doc: Int): Float = weight * child.score(doc)
    override fun count(doc: Int): Int = error("Weighted($weight).count()")
    override fun explain(doc: Int): Explanation {
        val orig = child.score(doc)
        if (child.matches(doc)) {
            return Explanation.match(weight*orig, "Weighted@$doc = $weight * $orig")
        } else {
            return Explanation.noMatch("Weighted.Miss@$doc (${weight*orig} = $weight * $orig)")
        }
    }
}

private class DirichletSmoothingEval(override val child: CountEvalNode, val mu: Double) : SingleChildEval<CountEvalNode>() {
    val background = mu * child.getCountStats().nonzeroCountProbability()
    override fun score(doc: Int): Float {
        val c = child.count(doc).toDouble()
        val length = child.length(doc).toDouble()
        return Math.log((c + background) / (length + mu)).toFloat()
    }
    override fun count(doc: Int): Int = TODO("not yet")
    override fun explain(doc: Int): Explanation {
        val c = child.count(doc)
        val length = child.length(doc)
        if (c > 0) {
            return Explanation.match(score(doc), "$c/$length with mu=$mu, bg=$background dirichlet smoothing. ${child.getCountStats()}", listOf(child.explain(doc)))
        } else {
            return Explanation.noMatch("score=${score(doc)} or $c/$length with mu=$mu, bg=$background dirichlet smoothing ${child.getCountStats()} ${child.getCountStats().nonzeroCountProbability()}.", listOf(child.explain(doc)))
        }
    }
}

fun main(args: Array<String>) {
    val dataset = DataPaths.Robust
    val qrels = dataset.getQueryJudgments()
    val queries = dataset.getTitleQueries()
    val evals = getEvaluators(listOf("ap", "ndcg"))
    val ms = NamedMeasures()

    dataset.getIndex().use { galago ->
        IreneIndex(IndexParams().apply {
            withPath(File("robust.irene2"))
        }).use { index ->
            val mu = index.getAverageDL("body")
            println(index.getStats(Term("body", "president")))

            queries.forEach { qid, qtext ->
                //if (qid != "301") return@forEach
                println("$qid $qtext")
                val qterms = index.analyzer.tokenize("body", qtext)
                val q = MeanExpr(qterms.map { DirQLExpr(TextExpr(it)) })

                val gq = GExpr("combine").apply { addTerms(qterms) }

                val top5 = galago.transformAndExecuteQuery(gq, pmake {
                    set("requested", 5)
                    set("annotate", true)
                    set("processingModel", "rankeddocument")
                }).scoredDocuments
                val topK = index.search(q, 1000)
                val results = topK.toQueryResults(index)

                //if (argp.get("printTopK", false)) {
                (0 until Math.min(5, topK.totalHits.toInt())).forEach { i ->
                    val id = results[i]
                    val gd = top5[i]

                    if (i == 0 && id.name != gd.name) {
                        println(gd.annotation)
                        val missed = index.documentById(gd.name)!!
                        println("Missed document=$missed")
                        println(index.explain(q, missed))
                    }

                    println("${id.rank}\t${id.name}\t${id.score}")
                    println("${gd.rank}\t${gd.name}\t${gd.score}")
                }
                //}

                val queryJudgments = qrels[qid]!!
                evals.forEach { measure, evalfn ->
                    val score = try {
                        evalfn.evaluate(results, queryJudgments)
                    } catch (npe: NullPointerException) {
                        System.err.println("NULL in eval...")
                        -Double.MAX_VALUE
                    }
                    ms.push("$measure.irene2", score)
                }

                println(ms.means())
            }
        }
        println(ms.means())
    }
}