package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.chai.IntList
import edu.umass.cics.ciir.iltr.RREnv
import edu.umass.cics.ciir.irene.DataNeeded
import edu.umass.cics.ciir.irene.GenericTokenizer
import edu.umass.cics.ciir.irene.WhitespaceTokenizer
import edu.umass.cics.ciir.sprf.pmake
import gnu.trove.map.hash.TObjectIntHashMap
import org.apache.lucene.index.Term
import org.apache.lucene.search.Explanation
import org.lemurproject.galago.core.eval.QueryJudgments
import org.lemurproject.galago.utility.Parameters

/**
 *
 * @author jfoley.
 */

class BagOfWords(terms: List<String>) {
    val counts = TObjectIntHashMap<String>()
    val length = terms.size.toDouble()
    val l2norm: Double by lazy {
        var sumSq = 0.0
        counts.forEachValue { c ->
            sumSq += c*c
            true
        }
        Math.sqrt(sumSq)
    }
    init {
        terms.forEach { counts.adjustOrPutValue(it, 1, 1) }
    }
    fun prob(term: String): Double = counts.get(term) / length
    fun count(term: String): Int {
        if (!counts.containsKey(term)) return 0
        return counts[term]
    }

}

interface ILTRDocField {
    val name: String
    val text: String
    val tokenizer: GenericTokenizer
    val terms: List<String>
    val freqs: BagOfWords
    val length: Int
    val uniqTerms: Int
    val termSet: Set<String>
    fun toEntry(): Pair<String, ILTRDocField> = Pair(name, this)
    fun count(term: String): Int
}

data class LTREmptyDocField(override val name: String) : ILTRDocField {
    override val text: String = ""
    override val tokenizer: GenericTokenizer = WhitespaceTokenizer()
    override val terms: List<String> = emptyList()
    override val freqs: BagOfWords = BagOfWords(emptyList())
    override val length: Int = 1
    override val uniqTerms: Int = 0
    override val termSet: Set<String> = emptySet()
    override fun count(term: String): Int = 0
}

data class LTRDocField(override val name: String, override val text: String, override val tokenizer: GenericTokenizer = WhitespaceTokenizer()) : ILTRDocField {
    override val terms: List<String> by lazy { tokenizer.tokenize(text, name) }
    override val freqs: BagOfWords by lazy { BagOfWords(terms) }
    override val length: Int get() = terms.size
    override val uniqTerms: Int get() = freqs.counts.size()
    override fun count(term: String): Int = freqs.count(term)
    override val termSet: Set<String> get() = freqs.counts.keySet()
}


data class LTRDoc(val name: String, val features: HashMap<String, Double>, val rank: Int, val fields: HashMap<String,ILTRDocField>, var defaultField: String = "document") {
    fun field(field: String): ILTRDocField = fields[field] ?: error("No such field: $field in $this.")
    fun terms(field: String) = field(field).terms
    fun freqs(field: String) = field(field).freqs

    constructor(name: String, text: String, field: String, tokenizer: GenericTokenizer = WhitespaceTokenizer()): this(name, hashMapOf(), -1, hashMapOf(LTRDocField(field, text, tokenizer).toEntry()), field)

    fun toJSONFeatures(qrels: QueryJudgments, qid: String) = pmake {
        set("label", qrels[name])
        set("qid", qid)
        set("features", Parameters.wrap(features))
        set("name", name)
    }
}

data class LTRDocScoringEnv(var document: LTRDoc? = null) : ScoringEnv(-1) {
    // Increment identifier if document changes.
    fun nextDocument(next: LTRDoc) {
        val current = document
        if (current == null || current.name != next.name) {
            doc++
            document = next
        }
    }
}

class LTREvalSetupContext(override val env: RREnv) : EvalSetupContext {
    val termCache = HashMap<Term, LTRDocTerm>()
    val lengthsCache = HashMap<String, LTRDocLength>()
    override fun create(term: Term, needed: DataNeeded): QueryEvalNode = termCache.computeIfAbsent(term, {
        LTRDocTerm(it.field(), it.text())
    })
    override fun createLengths(field: String): QueryEvalNode = lengthsCache.computeIfAbsent(field, { LTRDocLength(it) })
    override fun numDocs(): Int = 42

    /** TODO refactor this to be a createDoclistMatcher for [WhitelistExpr] as [WhitelistMatchEvalNode]. */
    override fun selectRelativeDocIds(ids: List<Int>): IntList = IntList(-1)
}


abstract class LTRDocFeatureNode : QueryEvalNode {
    lateinit var env: LTRDocScoringEnv
    val doc: LTRDoc get() = env.document!!
    override val children: List<QueryEvalNode> = emptyList()
    override fun init(env: ScoringEnv) {
        this.env = env as? LTRDocScoringEnv ?: error("Bad ScoringEnv given: $env")
    }
    override fun explain(): Explanation = if (matches()) {
        Explanation.match(score().toFloat(), "${this.javaClass.simpleName} count=${count()} score=${score()} matches=${matches()}")
    } else {
        Explanation.noMatch("${this.javaClass.simpleName} count=${count()} score=${score()} matches=${matches()}")
    }
    override fun estimateDF(): Long = 0
}

data class LTRDocLength(val field: String) : CountEvalNode, LTRDocFeatureNode() {
    override fun count(): Int = doc.field(field).length
    override fun matches(): Boolean = doc.fields.contains(field)
}

data class LTRDocTerm(val field: String, val term: String): PositionsEvalNode, LTRDocFeatureNode() {
    var cache: Pair<String, IntArray>? = null
    override fun positions(): PositionsIter {
        val c = cache
        if (c != null && doc.name == c.first) {
            return PositionsIter(c.second)
        }

        val vec = doc.field(field).terms
        val hits = vec.mapIndexedNotNull { i, t_i ->
            if (t_i == term) i else null
        }.toIntArray()
        cache = Pair(doc.name, hits)
        return PositionsIter(hits)
    }
    override fun count(): Int = doc.field(field).count(term)
    override fun matches(): Boolean = count() > 0
    override fun explain(): Explanation = if (matches()) {
        Explanation.match(score().toFloat(), "${this.javaClass.simpleName} count=${count()} score=${score()} matches=${matches()} positions=${positions()}")
    } else {
        Explanation.noMatch("${this.javaClass.simpleName} count=${count()} score=${score()} matches=${matches()} positions=${positions()}")
    }
}

data class LTRDocPositions(val field: String, val term: String): CountEvalNode, LTRDocFeatureNode() {
    override fun count(): Int = doc.field(field).count(term)
    override fun matches(): Boolean = count() > 0
}


