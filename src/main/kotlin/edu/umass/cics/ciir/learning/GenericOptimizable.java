package edu.umass.cics.ciir.learning;

import javax.annotation.Nonnull;

/**
 * @author jfoley
 */
public abstract class GenericOptimizable {
  protected final int numFeatures;
  protected final String measureName;

  public GenericOptimizable(int numFeatures, String measureName) {
    this.numFeatures = numFeatures;
    this.measureName = measureName;
  }

  public void beginOptimizing(int fid, @Nonnull double[] weights) { }
  public abstract double score(@Nonnull double[] weights);
}