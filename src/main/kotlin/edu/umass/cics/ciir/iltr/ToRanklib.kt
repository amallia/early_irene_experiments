package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.sprf.printer
import org.lemurproject.galago.utility.Parameters
import org.lemurproject.galago.utility.StreamCreator

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

    fun normalize(y: Double): Double {
        if (count == 0) return y
        return (y - min) / (max - min)
    }
}

/**
 * @author jfoley
 */
fun main(args: Array<String>) {
    val input = "robust.features.jsonl.gz"
    val output = "robust.features.ranklib"

    val fstats = HashMap<Pair<String, String>, FeatureStats>()
    StreamCreator.openInputStream(input).reader().useLines { lines ->
        lines.forEach { line ->
            val instance = Parameters.parseStringOrDie(line)
            val qid = instance.getString("qid")
            val features = instance.getMap("features")
            features.keys.forEach { fname ->
                fstats
                        .computeIfAbsent(Pair(qid, fname), { FeatureStats() })
                        .push(features.getDouble(fname))
            }
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

    StreamCreator.openOutputStream("$output").printer().use { out ->
        StreamCreator.openInputStream(input).reader().useLines { lines ->
            lines.forEach { line ->
                val instance = Parameters.parseStringOrDie(line)
                val qid = instance.getString("qid")
                val features = instance.getMap("features")
                val label = instance.getInt("label")
                val name = instance.getString("name")

                val pt = features.keys.associate { fname ->
                    val fid = fmap[fname]!!
                    val fval = fstats[Pair(qid, fname)]!!.normalize(features.getDouble(fname))
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

}