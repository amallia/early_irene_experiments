package edu.umass.cics.ciir.dbpedia

import edu.umass.cics.ciir.Debouncer
import edu.umass.cics.ciir.irene.IreneEnglishAnalyzer
import edu.umass.cics.ciir.irene.IreneIndexer
import edu.umass.cics.ciir.irene.tokenize
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.index.IndexableField
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File

/**
 *
 * @author jfoley.
 */

class MergedPageInfo(val name: String) {
    var anchorTexts = ArrayList<String>()
    var citationTitles = ArrayList<String>()
    var redirectTexts = ArrayList<String>()
    var person = HashMap<String, String>()
    var categories = HashSet<String>()
}
class PageInfoMap {
    val data = HashMap<String, MergedPageInfo>()
    operator fun get(id: String) = data.computeIfAbsent(id, { MergedPageInfo(it) })
}

object TestAnalyzer {
    @JvmStatic fun main(args: Array<String>) {
        val analyzer = IreneEnglishAnalyzer()
        println(analyzer.tokenize("body", "Lists_of_actors"))
        println(analyzer.tokenize("body", "Algeria/Transnational_Issues"))
    }
}

object MergeShardData {
    fun forEachJSONL(input: File, handle: (Parameters)->Unit) {
        var counted = 0
        val msg = Debouncer(2000)
        StreamCreator.openInputStream(input).bufferedReader().forEachLine { line ->
            counted++
            handle(Parameters.parseStringOrDie(line))
            if (msg.ready()) {
                println("Processed $counted from ${input.name}")
            }
        }
        println("Processed $counted from ${input.name}")
    }
    fun wikiTitleToText(xyz: String): String {
        val simplified = getCategory(xyz).replace('_', ' ')
        val withoutCamels = simplified.split(" ").map { words ->
            ProcessAndShardRelations.propToText(words.trim())
        }.joinToString(separator = " ") { it.trim() }

        if (simplified.toLowerCase() == withoutCamels) {
            return simplified
        }
        return simplified+" "+withoutCamels
    }
    @JvmStatic fun main(args: Array<String>) {
        IreneIndexer.build {
            withPath(File("dbpedia.irene2"))
            create()
            withAnalyzer("categories", WhitespaceAnalyzer())
        }.use { writer ->
            (0 until 20).forEach { shardNo ->
                val input = File("$dir/dbps/shard$shardNo")
                val data = PageInfoMap()

                forEachJSONL(File(input, "person_fields.jsonl.gz")) { json ->
                    val name = json.getString("id")!!
                    val keys = json.keys.toHashSet().apply { remove("id") }
                    val target = data[name]
                    keys.forEach { key ->
                        target.person[key] = json.getAsString(key)
                    }
                }
                println(data.data.size)
                forEachJSONL(File(input, "article_cites.jsonl.gz")) { json ->
                    val name = json.getString("id")!!
                    val text = json.getString("text") ?: name
                    data[name].citationTitles.add(text)
                }
                println(data.data.size)
                forEachJSONL(File(input, "anchor_texts.jsonl.gz")) { json ->
                    val name = json.getString("id")!!
                    val text = json.getString("text") ?: name
                    data[name].anchorTexts.add(text)
                }
                println(data.data.size)
                forEachJSONL(File(input, "article_categories.jsonl.gz")) { json ->
                    val name = json.getString("id")!!
                    val text = getCategory(json.getString("text")!!)
                    data[name].categories.add(text)
                }
                println(data.data.size)
                forEachJSONL(File(input, "article_categories.jsonl.gz")) { json ->
                    val dest = json.getString("id")!!
                    val src = json.getString("text")!!
                    data[dest].redirectTexts.add(wikiTitleToText(src))
                }
                println(data.data.size)

                // Stress RAM last, while writing to file:
                forEachJSONL(File(input, "full_text.jsonl.gz")) { json ->
                    val name = json.getString("id")!!
                    val text = json.getString("text") ?: name
                    val fields = arrayListOf<IndexableField>()
                    val docInfo = data[name]

                    val keepData = Field.Store.YES

                    fields.add(TextField("body", text, keepData))
                    fields.add(StringField("id", docInfo.name, keepData))

                    fields.add(TextField("anchor_text", docInfo.anchorTexts.joinToString(separator = "\n"), keepData))
                    fields.add(TextField("citation_titles", docInfo.citationTitles.joinToString(separator = "\n"), keepData))
                    fields.add(TextField("redirects", docInfo.redirectTexts.joinToString(separator = "\n"), keepData))
                    fields.add(TextField("categories", docInfo.categories.joinToString(separator = "\n"), keepData))
                    fields.add(TextField("categories_text", docInfo.categories.joinToString(separator = "\n") { wikiTitleToText(it)}, keepData))

                    val short = arrayListOf<String>()
                    val description = docInfo.person["description"]
                    if (description != null) { short.add(description) }
                    short.addAll(docInfo.anchorTexts)
                    short.addAll(docInfo.citationTitles)
                    short.addAll(docInfo.redirectTexts)
                    short.addAll(docInfo.categories.map { wikiTitleToText(it) })
                    // Don't keep amalgamation field.
                    fields.add(TextField("short_text", short.joinToString(separator="\n"), Field.Store.NO))

                    docInfo.person.forEach { field, value ->
                        when(field) {
                            "birthDate", "deathDate", "gender" -> fields.add(StringField(field, value, keepData))
                            "description", "birthPlace", "deathPlace" -> fields.add(TextField(field, value, keepData))
                            else -> fields.add(StringField("person_$field", value, keepData))
                        }
                    }

                    writer.push(fields)
                }
            }
        }
    }

    val ShortCategoryPrefix = "Category:"
    fun getCategory(input: String): String {
        if(input.startsWith(CategoryGraphAnalysis.CategoryPrefix)) {
            return input.substring(CategoryGraphAnalysis.CategoryPrefix.length)
        }
        if (input.startsWith(ShortCategoryPrefix)) {
            return input.substring(ShortCategoryPrefix.length)
        }
        return input
    }
}
