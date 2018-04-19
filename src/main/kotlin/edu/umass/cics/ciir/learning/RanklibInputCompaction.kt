package edu.umass.cics.ciir.learning

import edu.umass.cics.ciir.irene.utils.maybeSplitAt
import edu.umass.cics.ciir.irene.utils.smartLines
import edu.umass.cics.ciir.irene.utils.smartPrint
import edu.umass.cics.ciir.irene.utils.splitAt
import edu.umass.cics.ciir.iltr.pagerank.SpacesRegex
import org.lemurproject.galago.utility.Parameters
import java.io.File

/**
 * @author jfoley
 */
data class RanklibSparseInput(val label: Double, val qid: String, val name: String, val features: HashMap<Int, Float>, var prediction: Double = Double.NaN) {
    val relevant: Boolean get() = label > 0
}

fun parseRanklibSparseInput(line: String): RanklibSparseInput {
    val (data, name) = line.maybeSplitAt('#')
    if (name == null) error("Must have named documents!")
    val tokens = data.trim().split(SpacesRegex)
    val label = tokens[0].toDoubleOrNull() ?: error("First element must be label, found: ${tokens[0]}")
    if (!tokens[1].startsWith("qid:")) {
        error("Second token must be query identifier, found ${tokens[1]}")
    }
    val qid = tokens[1].substringAfter(':')
    val vector = HashMap<Int, Float>()
    for(i in (2 until tokens.size)) {
        val (fidStr, fvalStr) = tokens[i].splitAt(':') ?: error("Couldn't parse feature: ``${tokens[i]}'' ${tokens}")
        val fid = fidStr.toIntOrNull() ?: error("Couldn't parse feature id: $fidStr for token ${tokens[i]}")
        val fval = fvalStr.toFloatOrNull() ?: error("Couldn't parse feature value: $fvalStr for ${tokens[i]}")
        vector[fid] = fval
    }
    return RanklibSparseInput(label, qid, name, vector)
}


fun main(args: Array<String>) {
    val argp = Parameters.parseArgs(args)
    val input = File(argp.get("input", "/home/jfoley/code/savant-defects/extend_data/all.ranklib"))
    val output = File("${input.absolutePath}.abs")
    val uniqFeatures = HashSet<Int>()

    // 1st pass: collect present features.
    input.smartLines { lines ->
        for (line in lines) {
            val rli = parseRanklibSparseInput(line)
            uniqFeatures.addAll(rli.features.keys)
        }
    }

    // Re-number features
    val numbering = HashMap<Int, Int>()
    uniqFeatures.sorted().forEachIndexed { index, original ->
        numbering[original] = index+1
    }
    val kept = numbering.values.sorted()

    // 2nd pass: output data "fixed".
    output.smartPrint { out ->
        input.smartLines { lines ->
            for (line in lines) {
                val rli = parseRanklibSparseInput(line)
                val fvec = rli.features.mapKeys { (fid, _) -> numbering[fid]!! }
                val pt = fvec.toSortedMap().entries
                        .joinToString(
                                separator = " ",
                                prefix = "${rli.label} qid:${rli.qid} ",
                                postfix = " #${rli.name}"
                        ) { (fid, fval) -> "$fid:$fval" }
                out.println(pt)
            }
        }
    }
}