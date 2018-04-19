package edu.umass.cics.ciir.searchie

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.umass.cics.ciir.irene.utils.WeightedForHeap
import edu.umass.cics.ciir.irene.utils.pmap
import edu.umass.cics.ciir.irene.utils.smartLines
import edu.umass.cics.ciir.irene.ltr.RREnv
import edu.umass.cics.ciir.irene.ltr.RelevanceModel
import edu.umass.cics.ciir.irene.ltr.computeRelevanceModel
import edu.umass.cics.ciir.irene.ltr.toRRExpr
import edu.umass.cics.ciir.irene.GenericTokenizer
import edu.umass.cics.ciir.irene.galago.getStr
import edu.umass.cics.ciir.irene.galago.inqueryStop
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.scoring.BagOfWords
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import edu.umass.cics.ciir.irene.scoring.LTRTokenizedDocField
import edu.umass.cics.ciir.learning.Prediction
import edu.umass.cics.ciir.learning.computeAP
import edu.umass.cics.ciir.learning.computePrec
import edu.umass.cics.ciir.sprf.*
import gnu.trove.map.hash.TObjectDoubleHashMap
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.*

/**
 * @author jfoley
 */
val PositiveLabel: String = "LIST"
val OriginalScoreFeature: String = "original"
val LemmaFieldName = "lemma"

object SearchIENLP {
    val lemmatizer: StanfordCoreNLP by lazy {
        StanfordCoreNLP(Properties().apply {
            put("annotators", "tokenize, ssplit, pos, lemma")
        })
    }
    val tokenizer: GenericTokenizer by lazy { LemmaTokenizer() }
    private class LemmaTokenizer() : GenericTokenizer {
        override fun tokenize(input: String, field: String): List<String> {
            val ann = edu.stanford.nlp.pipeline.Annotation(input)
            lemmatizer.annotate(ann)
            val labels = ann.get(CoreAnnotations.TokensAnnotation::class.java)
            return labels.map { it.getString(CoreAnnotations.LemmaAnnotation::class.java) }
        }
    }

    @JvmStatic fun main(args: Array<String>) {
        println(tokenizer.tokenize("Welcome to the magical world of Dr. McMansion.", "default"))
    }
}

val includeFeatures = true

class TrecQADoc(val name: String, val score: Double, val qid: String, val targetDepth: Int, val relevant: Boolean, val sentences: List<TrecQASentence>) {
    init {
        sentences.forEach { it.doc = this }
    }

    val isPositive: Boolean by lazy { sentences.any { it.isPositive }}
    val lemmas: List<String> by lazy { sentences.flatMap { s -> s.tokens.map { it.lemma }  }}

    val ltrDoc: LTRDoc by lazy {
        val fname = LemmaFieldName
        val field = LTRTokenizedDocField(fname, lemmas, SearchIENLP.tokenizer)
        LTRDoc(name, hashMapOf(OriginalScoreFeature to score), hashMapOf(fname to field), defaultField = fname)
    }

    companion object {
        fun create(p: Parameters) = TrecQADoc(
                p.getStr("doc"),
                p.getDouble("score"),
                p.getStr("qid"),
                p.getInt("targetDepth"),
                p.isBoolean("docIsRelevant") && p.getBoolean("docIsRelevant"),
                p.getAsList("sentences", Parameters::class.java).map { TrecQASentence.create(it) })
    }
}

class TrecQASentence(val idx: Int, val tokens: List<TrecQAToken>) {
    val isPositive: Boolean by lazy { tokens.any { it.label == PositiveLabel } }
    lateinit var doc: TrecQADoc
    init {
        tokens.forEach { it.sentence = this }
    }
    val name: String get() = "${doc.name}.s$idx"

    val ltrDoc: LTRDoc by lazy {
        val fname = LemmaFieldName
        val field = LTRTokenizedDocField(fname, tokens.map { it.lemma }, SearchIENLP.tokenizer)
        LTRDoc(name, HashMap(), hashMapOf(fname to field), defaultField = fname)
    }

    override fun hashCode(): Int = doc.name.hashCode() * idx.hashCode()
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other is TrecQASentence) {
            return other.idx == idx && other.doc.name == doc.name
        }
        return false
    }

    companion object {
        fun create(p: Parameters) = TrecQASentence(
                p.getInt("sentenceId"),
                p.getAsList("tokens", Parameters::class.java).map { TrecQAToken.create(it) })
    }
}
class TrecQAToken(val label: String, val lemma: String, val token: String) {
    val features = HashSet<String>()
    var lemmaId = -1
    var middle = false
    var start = false
    var end = false
    lateinit var sentence: TrecQASentence
    val doc: TrecQADoc get() = sentence.doc
    val isPositive: Boolean = label == PositiveLabel

    companion object {
        fun create(p: Parameters): TrecQAToken {
            val tok = TrecQAToken(p.getStr("label"), p.getStr("lemma"), p.getStr("token"))
            tok.features.addAll(p.getStr("features").split(' '))
            tok.lemmaId = p.get("lemmaId", -1)
            tok.middle = p.get("tag-middle", false)
            tok.start = p.get("tag-start", false)
            tok.end = p.get("tag-end", false)
            return tok
        }
    }
}


fun loadQueryJSONL(fp: File): List<TrecQADoc> {
    fp.smartLines { lines ->
        return lines.toList().pmap { TrecQADoc.create(Parameters.parseString(it)) }
    }
}

val SearchIEDir = File(when(IRDataset.host) {
    "oakey" -> "/mnt/scratch/jfoley/searchie/trecqa"
    "sydney" -> "/mnt/nfs/work1/jfoley/alexandria/sigir17-trecqa/runs-18-jan-2017/gen1000"
    else -> notImpl(IRDataset.host)
})

val stopwords = setOf(".", ",", "", "'s", ":", "-lrb-", "-rrb-", "-lsb-", "-rsb-") + inqueryStop

data class TrecQAJudgment(val candidate: String, val correctness: Int, val doc: String) {
    val isPerfect: Boolean get() = correctness == 1
    val isUnsupported: Boolean get() = correctness == 2
    val isInexact: Boolean get() = correctness == 3
    val isFair: Boolean get() = isUnsupported || isInexact

    companion object {
        fun create(p: Parameters) = TrecQAJudgment(p.getStr("candidate"), p.getInt("correctness"), p.getStr("doc"))
    }
}
data class TrecQAQuery(val qid: String, val year: Int, val text: String, val answers: List<String>, val judgments: List<TrecQAJudgment>) {
    val prefectJudgments: List<TrecQAJudgment> get() = judgments.filter { it.isPerfect }

    companion object {
        fun create(p: Parameters) = TrecQAQuery(
                p.getStr("qid"),
                p.getInt("year"),
                    p.getStr("text"),
                p.getAsList("answers", String::class.java),
                p.getAsList("judgments", Parameters::class.java).map { TrecQAJudgment.create(it) }
        )
    }
}

fun main(args: Array<String>) {
    val queries = Parameters.parseFile("/home/jfoley/code/alexandria/data/trecqalist.json.gz").getAsList("queries", Parameters::class.java).map { TrecQAQuery.create(it) }.associateBy { it.qid }

    val wikiSource = WikiSource()
    val wiki = wikiSource.getIreneIndex()
    val argp = Parameters.parseArgs(args)
    val qfiles = SearchIEDir.listFiles().toList()
    val singleFile = qfiles.first { it.name.contains("80.6") }
    println(singleFile)
    val docs = loadQueryJSONL(singleFile)
    val qid = docs.first().qid
    val query = queries[qid]!!

    val fbDocs = 10
    val fbTerms = 50

    val correct = (query.answers + query.prefectJudgments.map { it.candidate }).toSet()
    println("Query: ${query.text} ${correct}")

    val ntoks = docs.sumBy { d -> d.sentences.sumBy { s -> s.tokens.size } }
    val nrelTok = docs.sumBy { d -> d.sentences.sumBy { s -> s.tokens.count { it.isPositive } } }
    val nrel = docs.count { it.relevant }

    println("qid: $qid, nrel: $nrel, npos: ${docs.count { it.isPositive }} ntok: $nrelTok/$ntoks")
    val sentencesList = docs.flatMap { it.sentences }
    val numRelevant = sentencesList.count { it.isPositive }

    val relevantTokens = sentencesList.filter { s -> s.isPositive }.flatMapTo(TreeSet()) { s ->
        s.tokens.filter { it.isPositive }.map { it.lemma.toLowerCase() }
    }
    println("Relevant Tokens: $relevantTokens")

    // Sentence-RM:
    val rm = computeRelevanceModel(docs.map { it.ltrDoc }, "original", fbDocs, LemmaFieldName, stopwords = stopwords)
    val rmExpr = rm.toQExpr(fbTerms, targetField = LemmaFieldName, statsField = wikiSource.statsField)

    val sranked = rankedSentences(wiki.env, rmExpr, sentencesList)
    // put feature in for computation of rm.
    sranked.forEach {
        it.sentence.ltrDoc.features["initial-rm"] = it.score
    }
    val sbDocs = 5
    val sbTerms = 10
    val sbLambda = 0.5
    val sRM = computeRelevanceModel(sentencesList.map { it.ltrDoc }, "initial-rm", sbDocs, LemmaFieldName, stopwords = stopwords)
    val sRMExpr = sRM.toQExpr(sbTerms, targetField = LemmaFieldName, statsField = wikiSource.statsField)

    val ap = rankAndComputeAP(wiki.env, rmExpr, sentencesList, numRelevant)
    println("AP\t$fbDocs\t$fbTerms\t$ap")
    val sAP = rankAndComputeAP(wiki.env, sRMExpr.mixed(sbLambda, rmExpr.deepCopy()), sentencesList, numRelevant)
    println("SRM-AP\t$sbLambda\t$sbDocs\t$sbTerms\t$sAP")

    val labelStep = 3
    val known = HashSet<TrecQASentence>()
    val remaining = HashSet<TrecQASentence>(sentencesList)
    for (iter in (0 until 20)) {
        val best = remaining.sortedByDescending { it.ltrDoc.features["latest"] }
        val newLabeled = best.take(labelStep)
        known.addAll(newLabeled)
        remaining.removeAll(newLabeled)

        val truthLeft = remaining.count { it.isPositive }
        if (truthLeft == 0) break

        val positives = known.filter { it.isPositive }
        val negatives = known.filterNot { it.isPositive }

        val mPos = toModel(positives, {term, repr -> repr.prob(term)})
        val mNeg = toModel(negatives, {term, repr -> repr.prob(term)})

        val hardQ = BagOfWords(positives.flatMap { s -> s.tokens.filter { it.isPositive }.map { it.lemma } })
        val labeledTerms = hardQ.toQExpr(hardQ.counts.size(), targetField = LemmaFieldName, statsField = wikiSource.statsField)

        val newModel = RocchioMerge(mPos, mNeg, 0.9)
        val expr = RelevanceModel(newModel, LemmaFieldName).toQExpr(fbTerms, statsField = wikiSource.statsField).mixed(known.size / numRelevant.toDouble(), labeledTerms)

        // This also updates "latest" for the next time around the loop.
        val overallRanked = rankedSentences(wiki.env, expr, sentencesList)
        val apAtStep = computeAP(overallRanked, numRelevant)
        val remainingRanked = rankedSentences(wiki.env, expr, remaining.toList())
        val succRank = remainingRanked.sorted().indexOfFirst { it.correct }
        println("AP-rocchio\t${known.size}\t$apAtStep\tP@$labelStep\t${computePrec(remainingRanked, labelStep)}\t${succRank}")

    }
}

fun <K> TObjectDoubleHashMap<K>.sumValues(): Double {
    var sum = 0.0
    this.forEachValue { x -> sum += x; true }
    return sum
}

fun RocchioMerge(positive: TObjectDoubleHashMap<String>, negative: TObjectDoubleHashMap<String>, lambda: Double): TObjectDoubleHashMap<String> {

    // normalize positive and negative in the scaling factors:
    val beta = lambda / positive.sumValues()
    val gamma = (1.0 - lambda) / negative.sumValues()

    val output = TObjectDoubleHashMap<String>(positive.size())
    positive.forEachKey { key ->
        val neg = negative[key] * gamma
        val pos = positive[key] * beta
        val diff = pos - neg
        if (diff > 0) {
            output.put(key, diff)
        }
        true
    }
    return output
}

fun toModel(docs: List<TrecQASentence>, termScorer: (String,BagOfWords)->Double): TObjectDoubleHashMap<String> {
    val mPos = TObjectDoubleHashMap<String>()
    docs.forEach { doc ->
        val repr = doc.ltrDoc.field(LemmaFieldName).freqs
        repr.counts.forEachKey { term ->
            val prob = termScorer(term, repr)
            mPos.adjustOrPutValue(term, prob, prob)
            true
        }
    }
    return mPos
}

data class ScoredSentence(override val score: Double, val sentence: TrecQASentence) : WeightedForHeap, Prediction {
    override val weight: Float get() = score.toFloat()
    override val correct: Boolean get() = sentence.isPositive
}

fun rankedSentences(env: RREnv, expr: QExpr, sentencesList: List<TrecQASentence>): List<ScoredSentence> {
    val scorer = expr.toRRExpr(env)
    return sentencesList.map { sentence ->
        val score = scorer.eval(sentence.ltrDoc)
        sentence.ltrDoc.features["latest"] = score
        ScoredSentence(score, sentence)
    }
}

fun rankAndComputeAP(env: RREnv, expr: QExpr, sentencesList: List<TrecQASentence>, numRelevant: Int): Double {
    val rawAPInst = rankedSentences(env, expr, sentencesList)
    return computeAP(rawAPInst, numRelevant)
}

/**
       Query: Identify nationalities of passengers on Flight 990 [American, Canadian, Chiliean, Egyptian, Sudanese, Syrian, Chilean, Syrians, CANADIAN, Egypt, Sudan, Syria, Chile, Canada, Americans, Egyptians, Canadians]
qid: 80.6, nrel: 0, npos: 12 ntok: 79/591465
Relevant Tokens: [american, americans, canada, canadian, canadians, chile, chilean, egypt, egyptian, egyptians, sudan, sudanese, syria]
AP	10	50	0.02146855030137368
SRM-AP	0.5	5	10	0.024925306130878067
AP-rocchio	3	0.05314093129135115	P@3	0.3333333333333333	2
AP-rocchio	6	0.05106781870193833	P@3	0.3333333333333333	0
AP-rocchio	9	0.06505836851056486	P@3	0.0	20
AP-rocchio	12	0.0647453077841286	P@3	0.0	21
AP-rocchio	15	0.0646253052253384	P@3	0.0	21
AP-rocchio	18	0.06462693352098857	P@3	0.0	18
AP-rocchio	21	0.0636732790289939	P@3	0.0	15
AP-rocchio	24	0.06256620515667478	P@3	0.0	14
AP-rocchio	27	0.06203936478953716	P@3	0.0	16
AP-rocchio	30	0.06150047166355095	P@3	0.0	22
AP-rocchio	33	0.060760495789456345	P@3	0.0	62
AP-rocchio	36	0.060323763567828814	P@3	0.0	53
AP-rocchio	39	0.06009878473561276	P@3	0.0	49
AP-rocchio	42	0.059973921656590524	P@3	0.0	45
AP-rocchio	45	0.05993846939849851	P@3	0.0	43
AP-rocchio	48	0.05990159081974252	P@3	0.0	41
AP-rocchio	51	0.059968692892744965	P@3	0.0	38
AP-rocchio	54	0.05287811590103285	P@3	0.0	35
AP-rocchio	57	0.04527660664370522	P@3	0.0	32
AP-rocchio	60	0.04211937567627327	P@3	0.0	29
        */

/**
        Features are actually garbage:

       Query: Identify nationalities of passengers on Flight 990 [American, Canadian, Chiliean, Egyptian, Sudanese, Syrian, Chilean, Syrians, CANADIAN, Egypt, Sudan, Syria, Chile, Canada, Americans, Egyptians, Canadians]
qid: 80.6, nrel: 0, npos: 12 ntok: 79/591465
Relevant Tokens: [american, americans, canada, canadian, canadians, chile, chilean, egypt, egyptian, egyptians, sudan, sudanese, syria]
AP	10	50	0.0032408950745832594
SRM-AP	0.5	5	10	0.0023867886533489938
AP-rocchio	3	0.0012556361068186699	P@3	0.0	22657
AP-rocchio	6	0.0012556361068186699	P@3	0.0	22654
AP-rocchio	9	0.0012556361068186699	P@3	0.0	22651
AP-rocchio	12	0.0012556361068186699	P@3	0.0	22648
AP-rocchio	15	0.0012556361068186699	P@3	0.0	22645
AP-rocchio	18	0.0012556361068186699	P@3	0.0	22642
AP-rocchio	21	0.0012556361068186699	P@3	0.0	22639
AP-rocchio	24	0.0012556361068186699	P@3	0.0	22636
AP-rocchio	27	0.0012556361068186699	P@3	0.0	22633
AP-rocchio	30	0.0012556361068186699	P@3	0.0	22630
AP-rocchio	33	0.0012556361068186699	P@3	0.0	22627
AP-rocchio	36	0.0012556361068186699	P@3	0.0	22624
AP-rocchio	39	0.0012556361068186699	P@3	0.0	22621
AP-rocchio	42	0.0012556361068186699	P@3	0.0	22618
AP-rocchio	45	0.0012556361068186699	P@3	0.0	22615
AP-rocchio	48	0.0012556361068186699	P@3	0.0	22612
AP-rocchio	51	0.0012556361068186699	P@3	0.0	22609
AP-rocchio	54	0.021471004016285468	P@3	0.0	34
AP-rocchio	57	0.02126330473925028	P@3	0.0	30
AP-rocchio	60	0.020887987596187648	P@3	0.0	35
        */