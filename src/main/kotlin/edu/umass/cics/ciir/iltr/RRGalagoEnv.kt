package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.QExpr
import edu.umass.cics.ciir.irene.toGalago
import edu.umass.cics.ciir.sprf.GExpr
import edu.umass.cics.ciir.sprf.setf
import org.lemurproject.galago.core.index.stats.FieldStatistics
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import org.lemurproject.galago.utility.Parameters

/**
 * @author jfoley
 */
class RRGalagoEnv(val retr: LocalRetrieval) : RREnv() {
    init {
        defaultField = "document"
    }
    val lengthsInfo = HashMap<String, FieldStatistics>()
    private fun getFieldStats(field: String): FieldStatistics {
        return lengthsInfo.computeIfAbsent(field, {retr.getCollectionStatistics(GExpr("lengths", field))})
    }

    override fun fieldStats(field: String): CountStats {
        val fstats = getFieldStats(field)
        return CountStats("field=$field", cf=0, df=0, dc=fstats.documentCount, cl=fstats.collectionLength)
    }

    override fun computeStats(q: QExpr): CountStats {
        val field = q.getSingleStatsField(defaultField)
        val stats = retr.getNodeStatistics(retr.transformQuery(q.toGalago(this), Parameters.create()))
        val fstats = getFieldStats(field)
        return CountStats(q.toString(),
                cf = stats.nodeFrequency,
                df = stats.nodeDocumentCount,
                dc = fstats.documentCount,
                cl = fstats.collectionLength)
    }
    override fun getStats(term: String, field: String?): CountStats {
        val termQ = GExpr("counts", term).apply { setf("field", field) }
        val stats = retr.getNodeStatistics(retr.transformQuery(termQ, Parameters.create()))!!
        val fstats = getFieldStats(field ?: defaultField)
        return CountStats(termQ.toString(),
                cf = stats.nodeFrequency,
                df = stats.nodeDocumentCount,
                dc = fstats.documentCount,
                cl = fstats.collectionLength)
    }
}