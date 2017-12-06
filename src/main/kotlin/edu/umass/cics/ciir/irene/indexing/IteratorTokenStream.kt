package edu.umass.cics.ciir.irene.indexing

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.LDoc
import edu.umass.cics.ciir.irene.tokenize
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute
import org.apache.lucene.document.Field
import org.apache.lucene.document.FieldType
import org.apache.lucene.document.NumericDocValuesField
import org.apache.lucene.document.StringField
import org.apache.lucene.index.IndexOptions
import org.apache.lucene.index.IndexableField
import org.apache.lucene.index.IndexableFieldType
import org.apache.lucene.util.BytesRef
import java.io.Reader

/**
 * @author jfoley
 */
class ListTokenStream(val items: List<String>) : TokenStream() {
    val length = items.size

    val term = addAttribute(CharTermAttribute::class.java)
    val offset = addAttribute(OffsetAttribute::class.java)
    /** Keep track of our offset into a "text" version of this field **/
    var pos = 0
    var charOffset = 0

    override fun reset() {
        pos = 0
        charOffset = 0
    }

    override fun incrementToken(): Boolean {
        if (pos < length) {
            val text = items[pos]
            term.setLength(0)
            term.append(text)
            offset.setOffset(charOffset, charOffset+text.length)
            // Step by text and 1 space.
            charOffset += text.length + 1
            pos++
            return true
        }
        return false
    }
}

class LDocBuilder(val params: IndexParams) {
    val YES = Field.Store.YES
    val NO = Field.Store.NO
    val fields = HashMap<String, List<IndexableField>>()
    val analyzer = params.analyzer

    fun setId(id: String) {
        if (fields.containsKey(params.idFieldName)) {
            error("Already specified ${params.idFieldName}=$id for this document, previous=${fields[params.idFieldName]!![0].stringValue()}.")
        }
        fields[params.idFieldName] = listOf(StringField(params.idFieldName, id, YES))
    }


    /**
     * This "setter" creates derived lucene fields from the same text.
     *  - A [AlreadyTokenizedTextField] of name [field] that contains a stream over the tokens using the correct analyzer.
     *  - A [NumericDocValuesField] of name "lengths:$[field]" which contains the true length; this lets Lucene use norms as it wishes.
     */
    fun setTextField(field: String, text: String, stored: Boolean=true) = setTextField(field, text, analyzer.tokenize(field, text), stored)

    fun maybeTextField(field: String, text: String?, stored: Boolean=true) {
        if (text != null) {
            setTextField(field, text, stored)
        }
    }

    /**
     * This "setter" creates derived lucene fields from the same text.
     *  - A [AlreadyTokenizedTextField] of name [field] that contains a stream over the tokens using the correct analyzer.
     *  - A [NumericDocValuesField] of name "lengths:$[field]" which contains the true length; this lets Lucene use norms as it wishes.
     *  - Unless you need the list of terms yourself, prefer the version that does not request it, this will invoke the analyzer for you.
     */
    fun setTextField(field: String, text: String, terms: List<String>, stored: Boolean) {
        if (fields.containsKey(field)) {
            error("Already specified $field for this document $fields.")
        }
        val length = terms.size
        val uniqLength = terms.toSet().size

        val keep = ArrayList<IndexableField>()

        keep.add(AlreadyTokenizedTextField(field, text, terms, stored))
        keep.add(NumericDocValuesField("lengths:$field", length.toLong()))
        keep.add(NumericDocValuesField("unique:$field", uniqLength.toLong()))
        fields[field] = keep
    }

    fun finish(): LDoc {
        if (!fields.containsKey(params.idFieldName)) {
            error("Generated document without an identifier field: ${params.idFieldName}!")
        }
        val ldoc = LDoc()
        fields.values.flatten().forEach { f -> ldoc.add(f) }
        //println(ldoc.fields.map { it.name() })
        return ldoc
    }
}

class AlreadyTokenizedTextField(
        val field: String,
        val text: String,
        val terms: List<String>,
        val stored: Boolean
) : IndexableField {
    override fun name(): String = field
    override fun stringValue(): String? = if (stored) { text } else null
    override fun numericValue(): Number? = null
    override fun binaryValue(): BytesRef? = null
    override fun readerValue(): Reader? = null
    override fun fieldType(): IndexableFieldType = if (stored) {
        storedType
    } else {
        notStoredType
    }
    override fun tokenStream(analyzer: Analyzer?, reuse: TokenStream?): TokenStream = ListTokenStream(terms)

    companion object {
        val notStoredType = FieldType().apply {
            setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            setTokenized(true);
            freeze();
        }
        val storedType = FieldType().apply {
            setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            setTokenized(true);
            setStored(true);
            freeze();
        }
    }
}
