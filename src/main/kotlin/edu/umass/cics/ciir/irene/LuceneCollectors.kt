package edu.umass.cics.ciir.irene

import edu.umass.cics.ciir.chai.ScoredForHeap
import edu.umass.cics.ciir.chai.ScoringHeap
import edu.umass.cics.ciir.irene.lang.MultiExpr
import edu.umass.cics.ciir.irene.scoring.IreneQueryScorer
import edu.umass.cics.ciir.irene.scoring.MultiEvalNode
import org.apache.lucene.index.LeafReaderContext
import org.apache.lucene.search.*

/**
 * @author jfoley
 */

data class IreneScoredDoc(
        override val score: Float,
        val doc: Int
) : ScoredForHeap {}

fun toTopDocs(heap: ScoringHeap<IreneScoredDoc>, totalSeen: Long=heap.totalSeen): TopDocs {
    val arr = heap.sorted.map { ScoreDoc(it.doc, it.score) }.toTypedArray()
    val max = arr.map { it.score }.max() ?: -Float.MAX_VALUE
    return TopDocs(totalSeen, arr, max)
}

class TopKCollectorManager(val requested: Int) : CollectorManager<TopKCollectorManager.TopKCollector, TopDocs> {
    override fun reduce(collectors: MutableCollection<TopKCollector>): TopDocs {
        val totalSeen = collectors.map { it.heap.totalSeen }.sum()

        val output = ScoringHeap<IreneScoredDoc>(requested)
        for (c in collectors) {
            for (d in c.heap.unsortedList) {
                output.offer(d)
            }
        }
        return toTopDocs(output, totalSeen)
    }

    inner class TopKCollector() : Collector {
        val heap = ScoringHeap<IreneScoredDoc>(requested)
        override fun needsScores(): Boolean = true
        override fun getLeafCollector(context: LeafReaderContext): LeafCollector {
            val docBase = context.docBase
            return object : LeafCollector {
                lateinit var query: Scorer
                override fun setScorer(scorer: Scorer) {
                    query = scorer
                }
                override fun collect(doc: Int) {
                    val score = query.score()
                    heap.offer(score, { IreneScoredDoc(score, doc+docBase)})
                }
            }
        }
    }

    override fun newCollector() = TopKCollector()
}

class PoolingCollectorManager(val mq: MultiExpr, val poolSize: Int): CollectorManager<PoolingCollectorManager.PoolingCollector, Map<String, TopDocs>> {
    val names = mq.names

    override fun reduce(collectors: Collection<PoolingCollector>): Map<String, TopDocs> {
        // Collect final top-$poolSize results per inner expression.
        val heaps = names.map { ScoringHeap<IreneScoredDoc>(poolSize) }
        val matches = names.mapTo(ArrayList()) { 0L }
        for (c in collectors) {
            c.heaps.forEachIndexed { i, heap ->
                matches[i] += heap.totalSeen
                for (d in heap.unsortedList) {
                    heaps[i].offer(d)
                }
            }
        }

        // patch up the "totalHits" part of the heaps:
        val topdocs = heaps.zip(matches).map { (heap, hits) -> toTopDocs(heap, hits) }
        return names.zip(topdocs).associate { it }
    }

    override fun newCollector() = PoolingCollector()

    inner class PoolingCollector : Collector {
        val heaps = names.map { ScoringHeap<IreneScoredDoc>(poolSize) }

        override fun needsScores(): Boolean = true
        override fun getLeafCollector(context: LeafReaderContext): LeafCollector {
            val docBase = context.docBase
            return object : LeafCollector {
                lateinit var eval: MultiEvalNode
                override fun setScorer(scorer: Scorer) {
                    eval = (scorer as IreneQueryScorer).eval as MultiEvalNode
                }
                override fun collect(doc: Int) {
                    val gdoc = doc + docBase
                    // TODO: offer a variant that scores all for any match.
                    // score any matching sub-expressions:
                    eval.children.forEachIndexed { i, node ->
                        if (node.matches(doc)) {
                            val score = node.score(doc)
                            heaps[i].offer(score, { IreneScoredDoc(score, gdoc) })
                        }
                    }
                }
            }
        }
    }
}