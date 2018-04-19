package edu.umass.cics.ciir.dbpedia

import edu.umass.cics.ciir.irene.utils.Debouncer
import edu.umass.cics.ciir.irene.BoolField
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
    val category: Boolean get() = name.startsWith("Category:")
    var label: String = ""
    var inLinks = ArrayList<String>()
    var outLinks = ArrayList<String>()
    var stringProps = ArrayList<String>()
    var objProps = ArrayList<String>()
    var anchorTexts = ArrayList<String>()
    var citationTitles = ArrayList<String>()
    var redirectTexts = ArrayList<String>()
    var person = HashMap<String, String>()
    var categories = HashSet<String>()

    fun joinWS(x: Collection<String>, titleToText: Boolean = false): String {
        if (titleToText) {
            return x.joinToString(separator = "\t") { wikiTitleToText(it) }
        } else {
            return x.joinToString(separator = "\t")
        }
    }
    fun processPage(writer: IreneIndexer, text: String?) {
        val fields = arrayListOf<IndexableField>()
        val keepData = Field.Store.YES
        val noKeepData = Field.Store.NO

        if (text != null) {
            fields.add(TextField("body", text, keepData))
        }
        if (label.isNotBlank()) {
            fields.add(TextField("title", wikiTitleToText(label), keepData))
        }
        fields.add(StringField("id", name, keepData))

        fields.add(BoolField("category", category, true))
        fields.add(BoolField("has_text", text != null, true))

        fields.add(TextField("anchor_text", joinWS(anchorTexts), keepData))
        fields.add(TextField("citation_titles", joinWS(citationTitles), keepData))
        fields.add(TextField("redirects", joinWS(redirectTexts), keepData))
        fields.add(TextField("categories", joinWS(categories), keepData))
        fields.add(TextField("categories_text", joinWS(categories, true), noKeepData))

        fields.add(TextField("str_props", joinWS(stringProps), keepData))
        fields.add(TextField("obj_props", joinWS(objProps), keepData))
        fields.add(TextField("props", joinWS(listOf(stringProps, objProps).flatten()), noKeepData))

        fields.add(TextField("inlinks", joinWS(inLinks), keepData))
        fields.add(TextField("outlinks", joinWS(outLinks), keepData))
        fields.add(TextField("links", joinWS(listOf(inLinks, outLinks).flatten()), noKeepData))

        val short = arrayListOf<String>()
        val description = person["description"]
        if (description != null) {
            short.add(description)
        }
        short.addAll(anchorTexts)
        short.addAll(citationTitles)
        short.addAll(redirectTexts)
        short.addAll(inLinks)
        short.addAll(outLinks)
        short.addAll(stringProps)
        short.addAll(objProps)
        short.addAll(categories.map { wikiTitleToText(it) })
        // Don't keep amalgamation field.
        fields.add(TextField("short_text", joinWS(short), Field.Store.NO))

        person.forEach { field, value ->
            when (field) {
                "birthDate", "deathDate", "gender" -> fields.add(StringField(field, value, keepData))
                "description", "birthPlace", "deathPlace" -> fields.add(TextField(field, value, keepData))
                else -> fields.add(StringField("person_$field", value, keepData))
            }
        }

        writer.push(fields)
    }
}
class PageInfoMap {
    val data = HashMap<String, MergedPageInfo>()
    operator fun get(id: String) = data.computeIfAbsent(id, { MergedPageInfo(it) })
    fun remove(id: String) = data.remove(id)
}

object TestAnalyzer {
    @JvmStatic fun main(args: Array<String>) {
        val analyzer = IreneEnglishAnalyzer()
        println(analyzer.tokenize("body", "Lists_of_actors"))
        println(analyzer.tokenize("body", "Algeria/Transnational_Issues"))
    }
}

object MergeShardData {
    var shardNumber = 0
    fun forEachJSONL(input: File, handle: (Parameters)->Unit) {
        var counted = 0
        val msg = Debouncer(2000)
        StreamCreator.openInputStream(input).bufferedReader().forEachLine { line ->
            counted++
            handle(Parameters.parseStringOrDie(line))
            if (msg.ready()) {
                println("Processed $counted from ${input.name} in shard $shardNumber")
            }
        }
        println("Processed $counted from ${input.name} in shard $shardNumber")
    }

    fun indexShard(writer: IreneIndexer, input: File) {
        val data = PageInfoMap()

        forEachJSONL(File(input, "titles.jsonl.gz")) { json ->
            val name = json.getString("id")!!
            val text = json.getString("text") ?: name
            data[name].label = text
        }
        forEachJSONL(File(input, "string_relations.jsonl.gz")) { json ->
            val name = json.getString("id")!!
            val text = json.getString("text") ?: name
            data[name].stringProps.add(text)
        }
        forEachJSONL(File(input, "object_relations.jsonl.gz")) { json ->
            val name = json.getString("id")!!
            val text = json.getString("text") ?: name
            data[name].objProps.add(text)
        }
        forEachJSONL(File(input, "link_relations.jsonl.gz")) { json ->
            val name = json.getString("id")!!
            if (json.isString("inlink")) {
                data[name].inLinks.add(json.getString("inlink")!!)
            } else if (json.isString("outlink")) {
                data[name].outLinks.add(json.getString("outlink")!!)
            }
        }

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
        forEachJSONL(File(input, "redirects.jsonl.gz")) { json ->
            val dest = json.getString("id")!!
            val src = json.getString("text")!!
            data[dest].redirectTexts.add(wikiTitleToText(src))
        }
        println(data.data.size)

        // Stress RAM last, while writing to file:
        forEachJSONL(File(input, "full_text.jsonl.gz")) { json ->
            val name = json.getString("id")!!
            val text = json.getString("text") ?: name
            val docInfo = data.remove(name) ?: MergedPageInfo(name)
            docInfo.processPage(writer, text)
        }

        println("Skip leftovers... ${data.data.size}")
        //data.data.values.forEach { leftover ->
            //leftover.processPage(writer, null)
        //}
    }
    @JvmStatic fun main(args: Array<String>) {
        IreneIndexer.build {
            withPath(File("dbpedia.irene2"))
            create()
            withAnalyzer("categories", WhitespaceAnalyzer())
        }.use { writer ->
            (0 until 20).forEach { shardNo ->
                shardNumber = shardNo
                val input = File("$dir/dbps/shard$shardNo")
                indexShard(writer, input)
            }
        }
    }


}


fun main(args: Array<String>) {
    IreneIndexer.build {
        withPath(File("dbpedia.shard0.irene2"))
        create()
        withAnalyzer("categories", WhitespaceAnalyzer())
    }.use { writer ->
        MergeShardData.indexShard(writer, File("$dir/shard0"))
    }
}
