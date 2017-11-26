package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.chai.Debouncer
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.unh.cs.treccar.Data
import edu.unh.cs.treccar.read_data.DeserializeData
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File

/**
 *
 * @author jfoley.
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val paragraphsInput = File(argp.get("input", "data/test200/train.test200.fold0.cbor.paragraphs"))
    val indexPath = File(argp.get("output", "data/test200.fold0.irene2"))

    val msg = Debouncer()
    // Seven million paragraphs (as described in paper)
    val total = argp.get("total", 7_000_000L)

    IreneIndexer.build {
        withPath(indexPath)
        create()
        defaultField = "document"
        withAnalyzer("links", WhitespaceAnalyzer())
    }.use { writer ->
        StreamCreator.openInputStream(paragraphsInput).use { input ->
            DeserializeData.iterParagraphs(input).forEach { paragraph: Data.Paragraph ->
                val id = paragraph.paraId
                val text = paragraph.textOnly
                val links = paragraph.entitiesOnly

                val processed = writer.push(
                        StringField("id", id, Field.Store.YES),
                        TextField("text", text, Field.Store.YES),
                        TextField("links", links.joinToString(separator="\t"), Field.Store.YES)
                )

                if (msg.ready()) {
                    println(id)
                    println(links)
                    println(text)
                    println(msg.estimate(processed, total))
                }
            }
        }
    }
}