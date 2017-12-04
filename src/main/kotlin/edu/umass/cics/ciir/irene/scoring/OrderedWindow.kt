package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.CountStatsStrategy
import edu.umass.cics.ciir.irene.lang.ProxExpr
import edu.umass.cics.ciir.irene.lang.SmallerCountExpr
import edu.umass.cics.ciir.irene.lang.UnorderedWindowCeilingExpr
import edu.umass.cics.ciir.irene.lang.UnorderedWindowExpr

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

fun countUnorderedWindows(iters: List<PositionsIter>, width: Int): Int {
    var hits = 0

    var max = iters[0].position + 1
    var min = iters[0].position
    for (i in 1 until iters.size) {
        val pos = iters[i].position
        max = Math.max(max, pos + 1)
        min = Math.min(min, pos)
    }

    while (true) {
        val match = (max - min <= width) || width == -1
        if (match) {
            //println("$min -> $max")
            hits++
        }

        val oldMin = min
        // now, reset bounds
        max = Integer.MIN_VALUE
        min = Integer.MAX_VALUE
        for (iter in iters) {
            var pos = iter.position
            if (pos == oldMin) {
                val notDone = iter.next()
                if (!notDone) {
                    return hits
                }
                assert(iter.position > oldMin)
            }
            pos = iter.position
            max = maxOf(max, pos + 1)
            min = minOf(min, pos)
        }
    }
}

/** Prox [ProxExpr] differs from [UnorderedWindowExpr] in that we want to compute something that has a stronger upper-bound. */
fun countProxWindows(iters: List<PositionsIter>, width: Int): Int {
    val smallest = iters.minBy { it.size } ?: return 0
    val rest = iters.filter { it !== smallest }

    var hits = 0
    smallest.forAll { candidate ->
        val min = candidate - width
        val max = candidate + width

        var match = true
        for (r in rest) {
            if (!r.advance(min)) return hits
            if (r.position > max) {
                match = false
                break
            }
        }
        if (match) {
            hits++
        }
    }
    return hits
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

    inline fun forAll(each: (Int)->Unit) {
        (0 until size).forEach { each(data[it]) }
    }
    /** @return true if found (false if [done]). */
    fun advance(target: Int): Boolean {
        while(index < size && data[index] < target) {
            index++
        }
        return index < size
    }

    override fun toString() = (0 until size).map { data[it] }.toList().toString()
}

abstract class CountWindow(val stats: CountStatsStrategy, children: List<PositionsEvalNode>) : AndEval<PositionsEvalNode>(children), CountEvalNode {
    init {
        assert(children.size > 1)
    }
    var lastDoc = -1
    var lastCount = 0
    abstract fun compute(iters: List<PositionsIter>): Int
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
        lastCount = compute(iters)
        return lastCount
    }
    override fun matches(doc: Int): Boolean {
        if (super.matches(doc)) {
            return count(doc) > 0
        }
        return false
    }
    override fun getCountStats(): CountStats = stats.get()
    override fun length(doc: Int): Int = children[0].length(doc)
}

class OrderedWindow(stats: CountStatsStrategy, children: List<PositionsEvalNode>, val step: Int) : CountWindow(stats, children) {
    override fun compute(iters: List<PositionsIter>): Int = countOrderedWindows(iters, step)
}

class UnorderedWindow(stats: CountStatsStrategy, children: List<PositionsEvalNode>, val width: Int) : CountWindow(stats, children) {
    override fun compute(iters: List<PositionsIter>): Int = countUnorderedWindows(iters, width)
}

/** From [ProxExpr] for computing something like [UnorderedWindow] but more realistically upper-bounded by MinCount(children). */
class ProxWindow(stats: CountStatsStrategy, children: List<PositionsEvalNode>, val width: Int): CountWindow(stats, children) {
    override fun compute(iters: List<PositionsIter>): Int = countProxWindows(iters, width)
}

/** From [SmallerCountExpr], for estimating the ceiling of [OrderedWindow] nodes. */
class SmallerCountWindow(val stats: CountStatsStrategy, children: List<CountEvalNode>) : AndEval<CountEvalNode>(children), CountEvalNode {
    init {
        assert(children.size > 1)
    }
    override fun count(doc: Int): Int {
        if (!matches(doc)) return 0
        var min = Integer.MAX_VALUE
        for (c in children) {
            val x = c.count(doc)
            if (x == 0) return 0
            if (x < min) min = x
        }
        return min
    }
    override fun getCountStats(): CountStats = stats.get()
    override fun length(doc: Int): Int = children[0].length(doc)
}

/** From [UnorderedWindowCeilingExpr], for estimating the ceiling of [UnorderedWindow] nodes.
 * Consider the terms (if two) as nodes in a bi-partite set, with counts p and q.
 * Therefore, the maximum possible output is the maximum number of nodes, or p * q.
 */
class UnorderedWindowCeiling(val stats: CountStatsStrategy, val width: Int, children: List<CountEvalNode>) : AndEval<CountEvalNode>(children), CountEvalNode {
    init {
        assert(children.size > 1)
    }
    override fun count(doc: Int): Int {
        if (!matches(doc)) return 0
        var max = 0
        var min = Integer.MAX_VALUE
        for (c in children) {
            val x = c.count(doc)
            if (x > max) max = x
            if (x < min) min = x
        }
        return max * minOf(min, width)
    }
    override fun getCountStats(): CountStats = stats.get()
    override fun length(doc: Int): Int = children[0].length(doc)
}
