package edu.umass.cics.ciir.irene

import org.junit.Assert
import org.junit.Test

/**
 * @author jfoley.
 */
class IreneQueryLanguageTest {
    val test = IreneQueryLanguage()
    @Test
    fun weightCombineTest() {
        val before = WeightExpr(WeightExpr(TextExpr("test"), 0.5), 2.0)
        val after = WeightExpr(TextExpr("test"), 1.0)
        Assert.assertEquals(after, test.simplify(before))
    }
    @Test
    fun manyWeightCombineTest() {
        val before = TextExpr("test").weighted(0.5).weighted(2.0).weighted(2.0).weighted(2.0)
        val after = WeightExpr(TextExpr("test"), 4.0)
        Assert.assertEquals(after, test.simplify(before))
    }

}