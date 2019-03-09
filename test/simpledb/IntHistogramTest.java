package simpledb;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;

import simpledb.Predicate.Op;

public class IntHistogramTest {

	/**
	 * Test to confirm that the IntHistogram implementation is constant-space
	 * (or, at least, reasonably small space; O(log(n)) might still work if
	 * your constants are good).
	 */
	@Test public void orderOfGrowthTest() {
		// Don't bother with a timeout on this test.
		// Printing debugging statements takes >> time than some inefficient algorithms.
		IntHistogram h = new IntHistogram(10000, 0, 100);
		
		// Feed the histogram more integers than would fit into our
		// 128mb allocated heap (4-byte integers)
		// If this fails, someone's storing every value...
		for (int c = 0; c < 33554432; c++) {
			h.addValue((c * 23) % 101);	// Pseudo-random number; at least get a distribution
		}
		
		// Try printing out all of the values; make sure "estimateSelectivity()"
		// cause any problems
		double selectivity = 0.0;
		for (int c = 0; c < 101; c++) {
			selectivity += h.estimateSelectivity(Op.EQUALS, c);
		}
		
		// All the selectivities should add up to 1, by definition.
		// Allow considerable leeway for rounding error, though 
		// (Java double's are good to 15 or so significant figures)
		Assert.assertTrue(selectivity > 0.99);
	}
	
	/**
	 * Test with a minimum and a maximum that are both negative numbers.
	 */
	@Test public void negativeRangeTest() {
		IntHistogram h = new IntHistogram(10, -60, -10);
		
		// All of the values here are negative.
		// Also, there are more of them than there are bins.
		for (int c = -60; c <= -10; c++) {
			h.addValue(c);
			h.estimateSelectivity(Op.EQUALS, c);
		}
		
		// Even with just 10 bins and 50 values,
		// the selectivity for this particular value should be at most 0.2.
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, -33) < 0.3);
		
		// And it really shouldn't be 0.
		// Though, it could easily be as low as 0.02, seeing as that's
		// the fraction of elements that actually are equal to -33.
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, -33) > 0.001);
	}
	
	/**
	 * Make sure that equality binning does something reasonable.
	 */
	@Test public void opEqualsTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		
		// This really should return "1.0"; but,
		// be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, 3) > 0.8);
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, 8) < 0.001);
		Assert.assertEquals(0F, h.estimateSelectivity(Op.EQUALS, 88), 0);
		Assert.assertEquals(0F, h.estimateSelectivity(Op.EQUALS, -50), 0);
	}
	
	/**
	 * Make sure that GREATER_THAN binning does something reasonable.
	 */
	@Test public void opGreaterThanTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, -1) > 0.999);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 2) > 0.6);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 4) < 0.4);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 12) < 0.001);
	}
	
	/**
	 * Make sure that LESS_THAN binning does something reasonable.
	 */
	@Test public void opLessThanTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, -1) < 0.001);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, 2) < 0.4);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, 4) > 0.6);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, 12) > 0.999);
	}
	
	/**
	 * Make sure that GREATER_THAN_OR_EQ binning does something reasonable.
	 */
	@Test public void opGreaterThanOrEqualsTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, -1) > 0.999);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 2) > 0.6);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 3) > 0.45);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 4) < 0.5);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 12) < 0.001);
	}
	
	/**
	 * Make sure that LESS_THAN_OR_EQ binning does something reasonable.
	 */
	@Test public void opLessThanOrEqualsTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, -1) < 0.001);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 2) < 0.4);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 3) > 0.45);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 4) > 0.6);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 12) > 0.999);
	}
	
	/**
	 * Make sure that equality binning does something reasonable.
	 */
	@Test public void opNotEqualsTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);

		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.NOT_EQUALS, 3) < 0.001);
		Assert.assertTrue(h.estimateSelectivity(Op.NOT_EQUALS, 8) > 0.01);
	}

	@Test public void toStringTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		h.addValue(1);
		h.addValue(5);
		h.addValue(5);
		h.addValue(9);
		Assert.assertEquals("1-1: 1, 2-2: 0, 3-3: 0, 4-4: 0, 5-5: 2, 6-6: 0, 7-7: 0, 8-8: 0, 9-9: 1, 10-10: 0", h.toString());

		IntHistogram h2 = new IntHistogram(5, 1, 10);
		h2.addValue(1);
		h2.addValue(5);
		h2.addValue(5);
		h2.addValue(9);
		Assert.assertEquals("1-2: 1, 3-4: 0, 5-6: 2, 7-8: 0, 9-10: 1", h2.toString());

		IntHistogram h3 = new IntHistogram(5, 0, 10);
		h3.addValue(1);
		h3.addValue(5);
		h3.addValue(5);
		h3.addValue(9);
		h3.addValue(10);
		Assert.assertEquals("0-2: 1, 3-5: 2, 6-8: 0, 9-11: 2, 12-14: 0", h3.toString());

		IntHistogram h4 = new IntHistogram(4, 0, 10);
		h4.addValue(1);
		h4.addValue(5);
		h4.addValue(5);
		h4.addValue(9);
		h4.addValue(10);
		Assert.assertEquals("0-2: 1, 3-5: 2, 6-8: 0, 9-11: 2", h4.toString());
	}
}
