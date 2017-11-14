package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.chai.smartDoLines
import edu.umass.cics.ciir.chai.smartPrint
import edu.umass.cics.ciir.sprf.getStr
import edu.umass.cics.ciir.sprf.incr
import edu.umass.cics.ciir.sprf.printer
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator
import java.io.File

class FeatureStats {
    var min = Double.MAX_VALUE
    var max = -Double.MAX_VALUE
    var sum = 0.0
    var count = 0
    fun push(x: Double) {
        count++
        sum += x
        if (x < min) {
            min = x;
        }
        if (x > max) {
            max = x
        }
    }

    override fun toString(): String = "$count $min..$max"

    fun normalize(y: Double): Double {
        if (count == 0) return y
        if (min == max) return y
        return (y - min) / (max - min)
    }
}

fun shouldNormalize(f: String): Boolean {
    val nonFieldName = f.substringAfter(":", f)
    return when (nonFieldName) {
        "qlen", "qstop", "docinfo", "avgwl", "docl", "jaccard-stop", "length" -> false
        else -> true
    }
}

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args);
    val dataset = argp.get("dataset", "wt10g")
    val limitFalse = argp.get("limitFalse", -1)
    val input = "l2rf/$dataset.features.jsonl.gz"
    val output = if (limitFalse < 0) {
        "l2rf/$dataset.features.ranklib"
    } else {
        "l2rf/$dataset.n$limitFalse.features.ranklib"
    }

    val fstats = HashMap<Pair<String, String>, FeatureStats>()
    var index = 0
    File(input).smartDoLines { line ->
        try {
            val instance = Parameters.parseStringOrDie(line)
            index++
            val qid = instance.getStr("qid")
            val features = instance.getMap("features")
            features.keys.forEach { fname ->
                fstats
                        .computeIfAbsent(Pair(qid, fname), { FeatureStats() })
                        .push(features.getDouble(fname))
            }
        } catch (e: Exception) {
            println("$index: $input: ")
            throw e
        }
    }

    // Define feature identifiers.
    val fmap: Map<String, Int> = fstats.keys
            .map { (_, fname) -> fname }
            .toSet().sorted()
            .mapIndexed { i,fname -> Pair(fname, i+1)} // start at 1, not 0
            .associate { it }

    StreamCreator.openOutputStream("$output.meta.json").printer().use { out ->
        out.println(Parameters.wrap(fmap).toPrettyString())
    }

    val negPerQuery = HashMap<String, Int>()

    File(output).smartPrint { out ->
        File(input).smartDoLines { line ->
            val instance = Parameters.parseStringOrDie(line)
            val qid = instance.getStr("qid")
            val features = instance.getMap("features")
            // Clue09 has negative labels, this annoys RankLib.
            val label = maxOf(0, instance.getInt("label"))
            val name = instance.getStr("name")

            if (limitFalse >= 0 && label == 0) {
                val count = negPerQuery.incr(qid, 1)
                if (count > limitFalse) {
                    return@smartDoLines
                }
            }

            val pt = fmap.keys.associate { fname ->
                val fid = fmap[fname]!!
                val rawVal = (features[fname] as Number?)?.toDouble();
                val stats = fstats[Pair(qid, name)]
                val fval = if (stats != null && shouldNormalize(fname)) {
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