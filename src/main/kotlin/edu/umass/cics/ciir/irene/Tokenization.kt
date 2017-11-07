package edu.umass.cics.ciir.irene

import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.LowerCaseFilter
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.en.KStemFilter
import org.apache.lucene.analysis.standard.StandardFilter
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

/**
 *
 * @author jfoley.
 */
class IreneEnglishAnalyzer : Analyzer() {
    override fun createComponents(fieldName: String?): TokenStreamComponents {
        val source = StandardTokenizer()
        var result: TokenStream = StandardFilter(source)
        result = LowerCaseFilter(result)
        result = KStemFilter(result)
        return TokenStreamComponents(source, result)
    }
}

fun Analyzer.tokenize(field: String, input: String): List<String> {
    val tokens = arrayListOf<String>()
    tokenStream(field, input).use { body ->
        val charTermAttr = body.addAttribute(CharTermAttribute::class.java)

        // iterate over tokenized field:
        body.reset()
        while(body.incrementToken()) {
            tokens.add(charTermAttr.toString())
        }
    }
    return tokens
}
