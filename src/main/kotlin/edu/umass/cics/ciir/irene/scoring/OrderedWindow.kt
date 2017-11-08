package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.LazyCountStats

/**
 *
 * @author jfoley.
 */
fun countOrderedWindows(arrayIterators: List<PositionsIter>, step: Int): Int {
    var hits = 0
    var notDone = true
    while (notDone) {
        // find the start of the first word
        var invalid = false

        // loop over all the rest of the words
        for (i in 1 until arrayIterators.size) {
            val end = arrayIterators[i - 1].position + 1

            // try to move this iterator so that it's past the end of the previous word
            assert(!arrayIterators[i].done)
            while (end > arrayIterators[i].position) {
                notDone = arrayIterators[i].next()

                // if there are no more occurrences of this word,
                // no more ordered windows are possible
                if (!notDone) {
                    return hits
                }
            }

            if (arrayIterators[i].position - end >= step) {
                invalid = true
                break
            }
        }

        // if it's a match, record it
        if (!invalid) {
            hits++
        }

        // move the first iterator forward - we are double dipping on all other iterators.
        notDone = arrayIterators[0].next()
    }

    return hits
}

class OrderedWindow(val stats: LazyCountStats, children: List<PositionsEvalNode>, val step: Int) : AndEval<PositionsEvalNode>(children), CountEvalNode {
    init {
        assert(children.size > 1)
        children.forEach {
            assert(it is PositionsEvalNode)
        }
        // TODO: someday error if there are different fields.
    }
    //val stats = children.map { it.getCountStats() }
    override fun score(doc: Int): Float = count(doc).toFloat()
    var lastDoc = -1
    var lastCount = 0
    override fun count(doc: Int): Int {
        if (doc == lastDoc) return lastCount

        // otherwise, compute!
        val iters = children.map {
            val count = it.count(doc)
            if (count == 0) {
                lastDoc = doc
                lastCount = 0
                return 0
            }
            it.positions(doc)
        }

        lastDoc = doc
        lastCount = countOrderedWindows(iters, step)
        return lastCount
    }

    override fun matches(doc: Int): Boolean {
        //println("OrderedWindow.matches($doc) = [super=${super.matches(doc)}, me=${count(doc)}, @$lastDoc,c=$lastCount")
        // all iterators have match?
        if (super.matches(doc)) {
            return count(doc) > 0
        }
        return false
    }

    override fun getCountStats(): CountStats = stats.get()
    override fun length(doc: Int): Int = children[0].length(doc)
}

class PositionsIter(val data: IntArray, val size: Int=data.size, var index: Int = 0) {
    val done: Boolean get() = index >= size
    fun reset() { index = 0}
    fun next(): Boolean {
        index++
        return !done
    }
    val position: Int get() = data[index]
    val count: Int get() = size

    override fun toString() = (0 until size).map { data[it] }.toList().toString()
}