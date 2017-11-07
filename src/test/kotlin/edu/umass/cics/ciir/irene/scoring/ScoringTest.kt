package edu.umass.cics.ciir.irene.scoring

import edu.umass.cics.ciir.iltr.LTRDoc
import edu.umass.cics.ciir.irene.*
import edu.umass.cics.ciir.sprf.GDoc
import edu.umass.cics.ciir.sprf.GExpr
import edu.umass.cics.ciir.sprf.pmake
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.junit.Assert
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.ExternalResource
import org.lemurproject.galago.core.index.mem.MemoryIndex
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import java.io.Closeable

/**
 * @author jfoley.
 */

fun <T> MutableMap<T,Int>.incr(x: T, amt: Int=1) {
    this.compute(x, { _, prev -> (prev ?: 0) + amt })
}

class CommonTestIndexes : Closeable {
    val doc1 = "the quick brown fox jumped over the lazy dog";
    val doc2 = "language modeling is the best";
    val doc3 = "the fox jumped the language of the brown dog";

    val idField = "id"
    val docNumberField = "docNo"
    val contentsField = "body"

    val docs = listOf(doc1,doc2,doc3)
    val ND: Int get() = docs.size
    val names = docs.indices.map {"doc${it+1}"}
    val gMemIndex = MemoryIndex(pmake {
        set("nonstemming", false)
        set("corpus", true)
    })
    val ltrIndex = ArrayList<LTRDoc>()
    val df = HashMap<String, Int>()
    val terms = HashSet<String>()

    lateinit var irene: IreneIndex
    lateinit var galago: LocalRetrieval
    init {
        val params = IndexParams().apply {
            inMemory()
            defaultAnalyzer = WhitespaceAnalyzer()
        }

        IreneIndexer(params).use { writer ->
            docs.forEachIndexed { i, doc ->
                writer.push(
                        StringField(idField, names[i], Field.Store.YES),
                        TextField(contentsField, doc, Field.Store.YES))

                val tokens = params.analyzer.tokenize("body", doc)

                val gdoc = GDoc()
                gdoc.name = names[i]
                gdoc.terms = tokens
                gdoc.text = doc
                gdoc.tags = emptyList()
                gMemIndex.process(gdoc)

                ltrIndex.add(LTRDoc(names[i], HashMap<String, Double>(), -1, doc))

                terms.addAll(tokens)
                tokens.toSet().forEach {
                    df.incr(it)
                }
            }
            writer.commit()
            irene = writer.open()
            galago = LocalRetrieval(gMemIndex)
        }
    }

    fun forEachTermPair(fn: (String,String) -> Unit) {
        val linear = terms.toList()
        linear.indices.forEach { i ->
            linear.indices.forEach { j ->
                if (i != j) {
                    fn(linear[i], linear[j])
                }
            }
        }
    }

    override fun close() {
        galago.close()
        ltrIndex.clear()
        irene.close()
    }
}

class CTIResource : ExternalResource() {
    var index: CommonTestIndexes? = null
    override fun before() { index = CommonTestIndexes() }
    override fun after() { index?.close() }
}

public class ScoringTest {
    public companion object {
        @ClassRule @JvmField
        public val resource = CTIResource()
    }

    val EPSILON = 0.0001
    inline fun dblEquals(x: Double, y: Double, orElse: ()->Unit) {
       if (Math.abs(x-y) > EPSILON) {
           orElse()
           Assert.assertEquals(x, y, EPSILON)
       }
    }

    @Test
    fun testAllStats() {
        val index = resource.index!!
        val bgStats = index.galago.getCollectionStatistics(GExpr("lengths"))
        index.terms.forEach { term ->
            val istats = index.irene.getStats(term)!!
            val gstats = index.galago.getNodeStatistics(GExpr("counts", term))!!
            Assert.assertEquals("cf $term", gstats.nodeFrequency, istats.cf)
            Assert.assertEquals("df $term", gstats.nodeDocumentCount, istats.df)
            Assert.assertEquals("dc $term", bgStats.documentCount, istats.dc)
            Assert.assertEquals("cl $term", bgStats.collectionLength, istats.cl)
        }
    }

    @Test
    fun testEquivQL() {
        val index = resource.index!!

        index.forEachTermPair { t1, t2 ->
            val gq = GExpr("combine").apply {
                addChild(GExpr.Text(t1))
                addChild(GExpr.Text(t2))
            }
            val iq = QueryLikelihood(listOf(t1, t2))

            val search = index.irene.search(iq, index.ND)
            val gres = index.galago.transformAndExecuteQuery(gq, pmake {
                set("requested", index.ND)
                set("annotate", true)
                set("processingModel", "rankeddocument")
            })
            val gTruth = gres.scoredDocuments.associate { Pair(it.name, it) }

            if (search.totalHits != gTruth.size.toLong()) {
                (0 until index.ND).forEach {
                    println(index.irene.explain(iq, it))
                }
            }

            Assert.assertEquals("$t1, $t2", search.totalHits, gTruth.size.toLong())

            search.scoreDocs.forEach { ldoc ->
                val name = index.irene.getDocumentName(ldoc.doc)!!
                val sameDoc = gTruth[name] ?: error("Galago returned different documents!")

                Assert.assertEquals(index.names[ldoc.doc], name)
                dblEquals(ldoc.score.toDouble(), sameDoc.score) {
                    println(sameDoc.annotation)
                    println(index.irene.explain(iq, ldoc.doc))
                }
            }

        }


    }

}