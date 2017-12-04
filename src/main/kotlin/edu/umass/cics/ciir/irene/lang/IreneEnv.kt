package edu.umass.cics.ciir.irene.lang

/**
 *
 * @author jfoley.
 */
class IreneQueryLanguage(val index: IIndex = EmptyIndex()) : RREnv() {
    override fun lookupNames(docNames: Set<String>): List<Int> = docNames.mapNotNull { index.documentById(it) }
    override fun fieldStats(field: String): CountStats = index.fieldStats(field) ?: error("Requested field $field does not exist.")
    override fun computeStats(q: QExpr): CountStats = index.getStats(q)
    override fun getStats(term: String, field: String?): CountStats = index.getStats(term, field ?: defaultField)
    val luceneQueryParser: QueryParser
        get() = QueryParser(defaultField, (index as IreneIndex).analyzer)
    override var estimateStats: String? = null
}

