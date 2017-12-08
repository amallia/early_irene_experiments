package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.chai.Fraction
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.irene.lang.*
import edu.umass.cics.ciir.sprf.*
import gnu.trove.set.hash.TIntHashSet
import org.apache.lucene.index.Term
import org.apache.lucene.search.DocIdSetIterator
import org.apache.lucene.search.Explanation
import java.io.File

class ScoringEnv(var doc: Int=-1) {
}

const val NO_MORE_DOCS = DocIdSetIterator.NO_MORE_DOCS

/**
 * Try not to implement this directly, use one of [RecursiveEval], if you expect to have children [SingleChildEval] if you have a single child, or [LeafEvalNode] if you expect to have no children.
 */
interface QueryEvalNode {
    val children: List<QueryEvalNode>
    // Return a score for a document.
    fun score(): Float
    // Return an count for a document.
    fun count(): Int
    // Return an boolean for a document. (must be true if you want this to be ranked).
    fun matches(): Boolean
    // Return an explanation for a document.
    fun explain(): Explanation

    // Used to accelerate AND and OR matching if accurate.
    fun estimateDF(): Long
    fun setHeapMinimum(target: Float) {}

    fun visit(fn: (QueryEvalNode)->Unit) {
        fn(this)
        for (c in children) {
            c.visit(fn)
        }
    }

    /**
    // Suggested implementation:
    // lateinit var env: ScoringEnv
    // fun init(env: ScoringEnv) { this.env = env }

     * Do not call [setup]. It recurses over init.
     */
    fun init(env: ScoringEnv)

    // Recursively calls [init] on tree.
    fun setup(env: ScoringEnv): ScoringEnv {
        visit { it.init(env) }
        return env
    }
}

abstract class LeafEvalNode : QueryEvalNode {
    lateinit var env: ScoringEnv
    override val children: List<QueryEvalNode> = emptyList()
    override fun init(env: ScoringEnv) { this.env = env }
}

internal class FixedMatchEvalNode(val matchAnswer: Boolean, override val child: QueryEvalNode): SingleChildEval<QueryEvalNode>() {
    override fun matches(): Boolean = matchAnswer
    override fun score(): Float = child.score()
    override fun count(): Int = child.count()
    override fun explain(): Explanation = if (matchAnswer) {
        Explanation.match(child.score(), "AlwaysMatchNode", child.explain())
    } else {
        Explanation.noMatch("NeverMatchNode", child.explain())
    }
}

internal class WhitelistMatchEvalNode(val allowed: TIntHashSet): LeafEvalNode() {
    val N = allowed.size().toLong()
    override fun estimateDF(): Long = N
    override fun matches(): Boolean = allowed.contains(env.doc)
    override fun score(): Float = if (matches()) { 1f } else { 0f }
    override fun count(): Int = if (matches()) { 1 } else { 0 }
    override fun explain(): Explanation = if (matches()) {
        Explanation.match(score(), "WhitelistMatchEvalNode N=$N")
    } else {
        Explanation.noMatch("WhitelistMatchEvalNode N=$N")
    }
}

// Going to execute this many times per document? Takes a while? Optimize that.
private class CachedQueryEvalNode(override val child: QueryEvalNode) : SingleChildEval<QueryEvalNode>() {
    var cachedScore = -Float.MAX_VALUE
    var cachedScoreDoc = -1
    var cachedCount = 0
    var cachedCountDoc = -1

    override fun score(): Float {
        if (env.doc == cachedScoreDoc) {
            cachedScoreDoc = env.doc
            cachedScore = child.score()
        }
        return cachedScore
    }

    override fun count(): Int {
        if (env.doc == cachedCountDoc) {
            cachedCountDoc = env.doc
            cachedCount = child.count()
        }
        return cachedCount
    }

    override fun explain(): Explanation {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

interface CountEvalNode : QueryEvalNode {
    override fun score() = count().toFloat()
    fun getCountStats(): CountStats
    fun length(): Int
}
interface PositionsEvalNode : CountEvalNode {
    fun positions(): PositionsIter
}

class ConstCountEvalNode(val count: Int, val lengths: CountEvalNode) : LeafEvalNode(), CountEvalNode {
    override fun count(): Int = count
    override fun matches(): Boolean = lengths.matches()
    override fun explain(): Explanation = Explanation.match(count.toFloat(), "ConstCountEvalNode", listOf(lengths.explain()))
    override fun estimateDF(): Long = lengths.estimateDF()
    override fun getCountStats(): CountStats = lengths.getCountStats()
    override fun length(): Int = lengths.length()
}

class ConstTrueNode(val numDocs: Int) : LeafEvalNode() {
    override fun setHeapMinimum(target: Float) { }
    override fun score(): Float = 1f
    override fun count(): Int = 1
    override fun matches(): Boolean = true
    override fun explain(): Explanation = Explanation.match(1f, "ConstTrueNode")
    override fun estimateDF(): Long = numDocs.toLong()
}

/**
 * Created from [ConstScoreExpr] via [exprToEval]
 */
class ConstEvalNode(val count: Int, val score: Float) : LeafEvalNode() {
    constructor(count: Int) : this(count, count.toFloat())
    constructor(score: Float) : this(1, score)

    override fun setHeapMinimum(target: Float) { }
    override fun score(): Float = score
    override fun count(): Int = count
    override fun matches(): Boolean = false

    override fun explain(): Explanation = Explanation.noMatch("ConstEvalNode(count=$count, score=$score)")
    override fun estimateDF(): Long = 0L
}

/**
 * Created from [RequireExpr] via [exprToEval]
 */
internal class RequireEval(val cond: QueryEvalNode, val score: QueryEvalNode, val miss: Float=-Float.MAX_VALUE): QueryEvalNode {
    lateinit var env: ScoringEnv
    override val children: List<QueryEvalNode> = listOf(cond, score)
    override fun score(): Float = if (cond.matches()) { score.score() } else miss
    override fun count(): Int = if (cond.matches()) { score.count() } else 0
    /**
     * Note: Galago semantics, don't look at whether score matches.
     * @see createOptimizedMovementExpr
     */
    override fun matches(): Boolean = cond.matches()
    override fun explain(): Explanation {
        val expls = listOf(cond, score).map { it.explain() }
        return if (cond.matches()) {
            Explanation.match(score.score(), "require-match", expls)
        } else {
            Explanation.noMatch("${score.score()} for require-miss", expls)
        }
    }
    override fun setHeapMinimum(target: Float) { score.setHeapMinimum(target) }
    override fun estimateDF(): Long = minOf(score.estimateDF(), cond.estimateDF())
    override fun init(env: ScoringEnv) { this.env = env }
}

/**
 * Helper class to generate Lucene's [Explanation] for subclasses of [AndEval] and  [OrEval] like [WeightedSumEval] or even [OrderedWindow].
 */
abstract class RecursiveEval<out T : QueryEvalNode>(override val children: List<T>) : QueryEvalNode {
    val className = this.javaClass.simpleName
    val N = children.size
    lateinit var env: ScoringEnv
    override fun explain(): Explanation {
        val expls = children.map { it.explain() }
        if (matches()) {
            return Explanation.match(score(), "$className.Match", expls)
        }
        return Explanation.noMatch("$className.Miss", expls)
    }
    override fun init(env: ScoringEnv) { this.env = env }
}

/**
 * Created from [MultiExpr] via [exprToEval].
 */
class MultiEvalNode(children: List<QueryEvalNode>, val names: List<String>) : OrEval<QueryEvalNode>(children) {
    val primary: Int = Math.max(0, names.indexOf("primary"))
    override fun count(): Int = children[primary].count()
    override fun score(): Float = children[primary].score()

    override fun explain(): Explanation {
        val expls = children.map { it.explain() }

        val namedChildExpls = names.zip(expls).map { (name, childExpl) ->
            if (childExpl.isMatch) {
                Explanation.match(childExpl.value, name, childExpl)
            } else {
                Explanation.noMatch("${childExpl.value} for name", childExpl)
            }
        }

        return Explanation.noMatch("MultiEvalNode ${names}", namedChildExpls)
    }
}

/**
 * Abstract class that knows how to match a set of children, optimized on their expected DF. Most useful query models are subclasses, e.g. [WeightedSumEval].
 */
abstract class OrEval<out T : QueryEvalNode>(children: List<T>) : RecursiveEval<T>(children) {
    val cost = children.map { it.estimateDF() }.max() ?: 0L
    val moveChildren = children.sortedByDescending { it.estimateDF() }
    override fun estimateDF(): Long = cost
    override fun matches(): Boolean {
        return moveChildren.any { it.matches() }
    }
}

/** Note that unlike in Galago, [AndEval] nodes do not perform movement. They briefly optimize to answer matches(doc) faster on average, but movement comes from a different query-program, e.g., [AndMover] where all leaf iterators only have doc information and are cheap copies as a result. */
abstract class AndEval<out T : QueryEvalNode>(children: List<T>) : RecursiveEval<T>(children) {
    val cost = children.map { it.estimateDF() }.min() ?: 0L
    val moveChildren = children.sortedBy { it.estimateDF() }
    override fun estimateDF(): Long = cost
    override fun matches(): Boolean {
        return moveChildren.all { it.matches() }
    }
}

/**
 * Created from [OrExpr] using [exprToEval]
 */
internal class BooleanOrEval(children: List<QueryEvalNode>): OrEval<QueryEvalNode>(children) {
    override fun score(): Float = if (matches()) { 1f } else { 0f }
    override fun count(): Int = if (matches()) { 1 } else { 0 }
}
/**
 * Created from [AndExpr] using [exprToEval]
 */
internal class BooleanAndEval(children: List<QueryEvalNode>): AndEval<QueryEvalNode>(children) {
    override fun score(): Float = if (matches()) { 1f } else { 0f }
    override fun count(): Int = if (matches()) { 1 } else { 0 }
}

/**
 * Created from [MaxExpr] using [exprToEval]
 */
internal class MaxEval(children: List<QueryEvalNode>) : OrEval<QueryEvalNode>(children) {
    override fun score(): Float {
        var sum = 0f
        children.forEach {
            sum = maxOf(sum, it.score())
        }
        return sum
    }
    override fun count(): Int {
        var sum = 0
        children.forEach {
            sum = maxOf(sum, it.count())
        }
        return sum
    }
    // Getting over the "min" is the same for any child of a max node.
    override fun setHeapMinimum(target: Float) {
        children.forEach { it.setHeapMinimum(target) }
    }
}

/**
 * Created from [CombineExpr] using [exprToEval]
 * Also known as #combine for you galago/indri folks.
 */
internal class WeightedSumEval(children: List<QueryEvalNode>, val weights: DoubleArray) : OrEval<QueryEvalNode>(children) {
    override fun score(): Float {
        return (0 until children.size).sumByDouble {
            weights[it] * children[it].score()
        }.toFloat()
    }

    override fun explain(): Explanation {
        val expls = children.map { it.explain() }
        if (matches()) {
            return Explanation.match(score(), "$className.Match ${weights.toList()}", expls)
        }
        return Explanation.noMatch("$className.Miss ${weights.toList()}", expls)
    }

    override fun count(): Int = error("Calling counts on WeightedSumEval is nonsense.")
    init { assert(weights.size == children.size, {"Weights provided to WeightedSumEval must exist for all children."}) }
}

/**
 * Helper class to make scorers that will have one child (like [DirichletSmoothingEval] and [BM25ScoringEval]) easier to implement.
 */
internal abstract class SingleChildEval<out T : QueryEvalNode> : QueryEvalNode {
    lateinit var env: ScoringEnv
    abstract val child: T
    override val children: List<QueryEvalNode> get() = listOf(child)
    override fun estimateDF(): Long = child.estimateDF()
    override fun matches(): Boolean = child.matches()
    override fun init(env: ScoringEnv) { this.env = env }
}

internal class WeightedEval(override val child: QueryEvalNode, val weight: Float): SingleChildEval<QueryEvalNode>() {
    override fun setHeapMinimum(target: Float) {
        // e.g., if this is 2*child, and target is 5
        // child target is 5 / 2
        child.setHeapMinimum(target / weight)
    }

    override fun score(): Float = weight * child.score()
    override fun count(): Int = error("Weighted($weight).count()")
    override fun explain(): Explanation {
        val orig = child.score()
        return if (child.matches()) {
            Explanation.match(weight*orig, "Weighted@${env.doc} = $weight * $orig", child.explain())
        } else {
            Explanation.noMatch("Weighted.Miss@${env.doc} (${weight*orig} = $weight * $orig)", child.explain())
        }
    }
}

// TODO, someday re-implement idf as transform to WeightedExpr() and have BM25InnerEval()
/**
 * Created from [BM25Expr] via [exprToEval]
 */
internal class BM25ScoringEval(override val child: CountEvalNode, val b: Double, val k: Double): SingleChildEval<CountEvalNode>() {
    private val stats = child.getCountStats()
    private val avgDL = stats.avgDL()
    private val idf = Math.log(stats.dc / (stats.df + 0.5))

    override fun score(): Float {
        val count = child.count().toDouble()
        val length = child.length().toDouble()
        val num = count * (k+1.0)
        val denom = count + (k * (1.0 - b + (b * length / avgDL)))
        return (idf * (num / denom)).toFloat()
    }

    override fun count(): Int = error("count() not implemented for ScoreNode")
    override fun explain(): Explanation {
        val c = child.count()
        val length = child.length()
        if (c > 0) {
            return Explanation.match(score(), "$c/$length with b=$b, k=$k with BM25. ${stats}", listOf(child.explain()))
        } else {
            return Explanation.noMatch("score=${score()} or $c/$length with b=$b, k=$k with BM25. ${stats}", listOf(child.explain()))
        }
    }
}

/**
 * Created from [DirQLExpr] via [exprToEval]
 */
internal class DirichletSmoothingEval(override val child: CountEvalNode, val mu: Double) : SingleChildEval<CountEvalNode>() {
    val background = mu * child.getCountStats().nonzeroCountProbability()
    init {
        assert(java.lang.Double.isFinite(background)) {
            "child.stats=${child.getCountStats()}"
        }
    }
    override fun score(): Float {
        val c = child.count().toDouble()
        val length = child.length().toDouble()
        return Math.log((c + background) / (length + mu)).toFloat()
    }
    override fun count(): Int = TODO("not yet")
    override fun explain(): Explanation {
        val c = child.count()
        val length = child.length()
        if (c > 0) {
            return Explanation.match(score(), "$c/$length with mu=$mu, bg=$background dirichlet smoothing. ${child.getCountStats()}", listOf(child.explain()))
        } else {
            return Explanation.noMatch("score=${score()} or $c/$length with mu=$mu, bg=$background dirichlet smoothing ${child.getCountStats()} ${child.getCountStats().nonzeroCountProbability()}.", listOf(child.explain()))
        }
    }
}

object DirichletSmoothingExploration {
    @JvmStatic fun main(args: Array<String>) {
        // As length increases, so does the Dirichlet Probability.
        // As frequency increases, so does the Dirichlet Probability.
        // Need max length.
        // Estimate max freq as a fraction of that max length?
        val bg = 0.05
        val mu = 1500.0
        (0 .. 100).forEach { i ->
            val f = Fraction(i,100)
            val values = (0 .. 10).map { s ->
                val count = (f.numerator * s).toDouble()
                val length = (f.denominator * s).toDouble()

                Math.log((count + bg) / (length + mu))
            }

            println("${f.numerator}/${f.denominator} ${values.joinToString { "%1.3f".format(it) }}")
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