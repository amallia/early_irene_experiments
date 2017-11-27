package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.chai.FeatureStats
import edu.umass.cics.ciir.chai.smartDoLines
import edu.umass.cics.ciir.chai.smartPrint
import edu.umass.cics.ciir.sprf.getStr
import edu.umass.cics.ciir.sprf.printer
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File


fun shouldNormalize(f: String): Boolean = f.startsWith("norm:")

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args);
    val dataset = argp.get("dataset", "trec-car-test200")
    val input = argp.get("input", "l2rf/latest/$dataset.features.jsonl.gz")
    val docInput = File("html_raw/$dataset.features.jsonl.gz")
    val output = argp.get("output", "l2rf/latest/$dataset.features.ranklib")

    val docFeatures = HashMap<String, Map<String, Double>>()
    if (docInput.exists()) {
        docInput.smartDoLines(true) { line ->
            val p = Parameters.parseStringOrDie(line)
            val docId = p.getStr("id")
            val features = p.getMap("features")
            val docFVec = HashMap<String, Double>()
            docFeatures.put(docId, docFVec)
            features.keys.forEach { key ->
                val value = features[key]
                when (value) {
                    null -> docFVec[key] = 0.0
                    is Number -> docFVec[key] = value.toDouble()
                    is Boolean -> docFVec[key] = if (value) 1.0 else 0.0
                    else -> error("Unhandled value $value")
                }
            }
        }
    }

    println("Finished DocFeatures")

    val allFeatures = HashSet<String>()
    val fstats = HashMap<Pair<String, String>, FeatureStats>()
    var index = 0
    File(input).smartDoLines() { line ->
        try {
            index++
            val instance = Parameters.parseStringOrDie(line)
            val qid = instance.getStr("qid")
            val features = instance.getMap("features")
            val name = instance.getStr("name")
            features.keys.forEach { fname ->
                allFeatures.add(fname)
                if (shouldNormalize(fname)) {
                    fstats
                            .computeIfAbsent(Pair(qid, fname), { FeatureStats() })
                            .push(features.getDouble(fname))
                }
            }
            docFeatures[name]?.forEach { fname, value ->
                allFeatures.add(fname)
                /*if (shouldNormalize(fname)) {
                    fstats
                            .computeIfAbsent(Pair(qid, fname), { FeatureStats() })
                            .push(value)
                }*/
            }
        } catch (e: Exception) {
            println("$index: $input: ")
            throw e
        }
    }

    // Define feature identifiers.
    val fmap: Map<String, Int> = allFeatures
            .sorted()
            .mapIndexed { i,fname -> Pair(fname, i+1)} // start at 1, not 0
            .associate { it }

    println(allFeatures)
    println(fmap)
    println(fstats)

    StreamCreator.openOutputStream("$output.meta.json").printer().use { out ->
        out.println(Parameters.wrap(fmap).toPrettyString())
    }

    File(output).smartPrint { out ->
        File(input).smartDoLines() { line ->
            val instance = Parameters.parseStringOrDie(line)
            val qid = instance.getStr("qid")
            val features = instance.getMap("features")
            // Clue09 has negative labels, this annoys RankLib.
            val label = maxOf(0, instance.getInt("label"))
            val name = instance.getStr("name")

            val docFVec = docFeatures[name] ?: emptyMap()

            val pt = fmap.entries.associate { (fname, fid) ->
                val rawVal = (features[fname] as Number?)?.toDouble() ?: docFVec[fname]
                val stats = fstats[Pair(qid, fname)]
                val fval = if (stats != null) {
                    if (rawVal == null) 0.0 else {
                        stats.normalize(rawVal)
                    }
                } else rawVal ?: 0.0
                Pair(fid, fval)
            }.toSortedMap().entries
                    .joinToString(
                            separator = " ",
                            prefix = "$label qid:$qid ",
                            postfix = " #$name"
                    ) { (fid, fval) -> "$fid:$fval" }
            out.println(pt)
            if (label > 0) {
                println(pt)
            }
        }
    }

}