package edu.umass.cics.ciir.irene

import org.apache.lucene.search.TopDocs
import org.lemurproject.galago.core.eval.QueryResults
import org.lemurproject.galago.core.eval.SimpleEvalDoc

/**
 *
 * @author jfoley.
 */

fun TopDocs.toQueryResults(index: IreneIndex) = QueryResults(this.scoreDocs.mapIndexed { i, sdoc ->
    val name = index.getDocumentName(sdoc.doc) ?: "null"
    SimpleEvalDoc("", i+1, sdoc.score.toDouble())
})