package edu.umass.cics.ciir.iltr

import edu.umass.cics.ciir.irene.CountStats
import edu.umass.cics.ciir.irene.QExpr
import edu.umass.cics.ciir.irene.TextExpr
import edu.umass.cics.ciir.irene.toGalago
import edu.umass.cics.ciir.sprf.GExpr
import org.lemurproject.galago.core.index.stats.FieldStatistics
import org.lemurproject.galago.core.retrieval.LocalRetrieval
import org.lemurproject.galago.utility.Parameters

/**
 * @author jfoley
 */
class RRGalagoEnv(val retr: LocalRetrieval) : RREnv() {
    val lengthsInfo = HashMap<String, FieldStatistics>()
    private fun getFieldStats(field: String): FieldStatistics {
        return lengthsInfo.computeIfAbsent(field, {retr.getCollectionStatistics(GExpr("lengths", field))})
    }

    override fun computeStats(q: QExpr): CountStats {
        val field = q.getSingleField(defaultField)
        val stats = retr.getNodeStatistics(retr.transformQuery(q.toGalago(), Parameters.create()))
        val fstats = getFieldStats(field)
        return CountStats(q.toString(),
                cf = stats.nodeFrequency,
                df = stats.nodeDocumentCount,
                dc = fstats.documentCount,
                cl = fstats.collectionLength)

    }
    override fun getStats(term: String, field: String?): CountStats =  computeStats(TextExpr(term, field ?: defaultField))
}