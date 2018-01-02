package edu.umass.cics.ciir.searchie

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.umass.cics.ciir.chai.ScoredForHeap
import edu.umass.cics.ciir.chai.pmap
import edu.umass.cics.ciir.chai.smartLines
import edu.umass.cics.ciir.iltr.RREnv
import edu.umass.cics.ciir.iltr.computeRelevanceModel
import edu.umass.cics.ciir.iltr.toRRExpr
import edu.umass.cics.ciir.irene.GenericTokenizer
import edu.umass.cics.ciir.irene.lang.QExpr
import edu.umass.cics.ciir.irene.scoring.LTRDoc
import edu.umass.cics.ciir.irene.scoring.LTRTokenizedDocField
import edu.umass.cics.ciir.learning.SimplePrediction
import edu.umass.cics.ciir.learning.computeAP
import edu.umass.cics.ciir.sprf.*
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
        it.sentence.ltrDoc.features["latest"] = it.score.toDouble()
    }
    val sbDocs = 5
    val sbTerms = 10
    val sbLambda = 0.5
    val sRM = computeRelevanceModel(sentencesList.map { it.ltrDoc }, "latest", sbDocs, LemmaFieldName, stopwords = stopwords)
    val sRMExpr = sRM.toQExpr(sbTerms, targetField = LemmaFieldName, statsField = wikiSource.statsField)

    val sAP = rankAndComputeAP(wiki.env, sRMExpr.mixed(sbLambda, rmExpr.deepCopy()), sentencesList, numRelevant)
    println("SRM-AP\t$sbLambda\t$sbDocs\t$sbTerms\t$sAP")

    val ap = rankAndComputeAP(wiki.env, rmExpr, sentencesList, numRelevant)
    println("AP\t$fbDocs\t$fbTerms\t$ap")
}

data class ScoredSentence(override val score: Float, val sentence: TrecQASentence) : ScoredForHeap {
    fun toPrediction() = SimplePrediction(score.toDouble(), sentence.isPositive)
}

fun rankedSentences(env: RREnv, expr: QExpr, sentencesList: List<TrecQASentence>): List<ScoredSentence> {
    val scorer = expr.toRRExpr(env)
    return sentencesList.map { sentence ->
        val score = scorer.eval(sentence.ltrDoc)
        ScoredSentence(score.toFloat(), sentence)
    }
}

fun rankAndComputeAP(env: RREnv, expr: QExpr, sentencesList: List<TrecQASentence>, numRelevant: Int): Double {
    val rawAPInst = rankedSentences(env, expr, sentencesList).map { it.toPrediction() }
    return computeAP(rawAPInst, numRelevant)
}
