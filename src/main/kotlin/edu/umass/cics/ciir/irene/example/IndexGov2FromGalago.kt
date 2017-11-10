package edu.umass.cics.ciir.irene.example

import edu.umass.cics.ciir.irene.IndexParams
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.sprf.DataPaths
import edu.umass.cics.ciir.sprf.forAllGDocs
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexableField
import org.jsoup.Jsoup
import org.lemurproject.galago.utility.Parameters
import java.io.File
import java.util.logging.FileHandler
import java.util.logging.Level
import java.util.logging.Logger
import java.util.logging.SimpleFormatter

/**
 * @author jfoley
 */

fun FileLogger(path: String) = Logger.getAnonymousLogger().apply {
    useParentHandlers = false
    addHandler(FileHandler(path).apply {
        formatter = SimpleFormatter()
    })
}

val PunctuationRegex = "\\p{Punct}".toRegex()

fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)

    val indexF = File(argp.get("path", "/mnt/scratch/jfoley/gov2.irene2"))
    val params = IndexParams().apply {
        create()
        withPath(indexF)
        withAnalyzer("url", WhitespaceAnalyzer())
    }
    val logger = FileLogger(argp.get("logger", "${indexF.absolutePath}.log"))

    IreneIndexer(params).use { writer ->
        DataPaths.Gov2.getIndex().use { retr ->
            retr.forAllGDocs { gdoc ->
                val id = gdoc.name!!
                val meta = gdoc.metadata ?: emptyMap()
                val url = meta["url"]
                val text = gdoc.text
                val html = try {
                    Jsoup.parse(text)
                } catch (e: Throwable) {
                    logger.log(Level.WARNING, "Jsoup Exception", e)
                    return@forAllGDocs
                }

                val doc = arrayListOf<IndexableField>()
                doc.add(StringField("id", id, Field.Store.YES))
                if (!url.isNullOrBlank()) {
                    doc.add(StringField("stored-url", url, Field.Store.YES))
                    val tokenized = url!!.split(PunctuationRegex).joinToString(" ")
                    doc.add(TextField("url", tokenized, Field.Store.YES))
                }

                val title = html.title()
                if (!title.isNullOrBlank()) {
                    doc.add(TextField("title", title, Field.Store.YES))
                }
                val body = html.body()?.text()
                if (!body.isNullOrBlank()) {
                    doc.add(TextField("body", body, Field.Store.YES))
                }
                doc.add(TextField("document", html.text(), Field.Store.YES))
                //println("$id $title $url")
            }
        }
    }
}