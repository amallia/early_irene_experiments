package edu.umass.cics.ciir.learning;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

/**
 * This class implements the linear ranking model known as Coordinate Ascent. It was proposed in this paper:
 *  D. Metzler and W.B. Croft. Linear feature-based models for information retrieval. Information Retrieval, 10(3): 257-274, 2007.
 * @author vdang, jfoley
 */
public class GenericOptimizer {
  //Parameters
  public int nRestart = 5;
  public int nMaxIteration = 25;
  public double stepBase = 0.05;
  public double stepScale = 2.0;
  public double tolerance = 0.001;
  public double maxPerformance = Double.MAX_VALUE;
  public boolean regularized = false;
  public double slack = 0.001;//regularized parameter
  /** If more than just the sign of the dot product matters, set this to false */
  public boolean normalize = true;
  /** For each restart, should we randomize weights? */
  public boolean randomizeWeights = true;
  public PrintStream output = System.out;

  public Random rand;

  //Local variables
  public double[] weight = null;

  public final GenericOptimizable evaluator;

  public GenericOptimizer(GenericOptimizable evaluator) {
    this(evaluator, new Random());
  }
  public GenericOptimizer(GenericOptimizable evaluator, Random rand) {
    this.evaluator = evaluator;
    this.rand = rand;
    init();
  }

  private void init() {
    int N = getNumFeatures();
    weight = new double[N];
    clearWeightVector();
    Arrays.fill(weight, 1.0 / N);
  }

  private void clearWeightVector() {
    int N = getNumFeatures();
    if(this.randomizeWeights) {
      for (int i = 0; i < weight.length; i++) {
        weight[i] = rand.nextGaussian();
        //if(evaluator.constraints.get(i).valid(weight[i])) break; // TODO, solve this better...
      }
    } else {
      Arrays.fill(weight, 1.0 / N);
    }
  }

  public int getNumFeatures() {
    return evaluator.numFeatures;
  }

  public static void copy(double[] source, double[] target) {
    //for(int j=0;j<source.length;j++)
      //target[j] = source[j];
    System.arraycopy(source, 0, target, 0, source.length);
  }
  public void PRINTLN(String s) { output.println(s); }
  public void PRINT(String s) { output.print(s); }

  public double score() {
    //double startScore = scorer.score(rank(samples));//compute all the scores (in whatever metric specified) and store them as cache
    return evaluator.score(weight);
  }

  public int[] getShuffledFeatures() {
    ArrayList<Integer> features = new ArrayList<>();
    for (int i = 0; i < getNumFeatures(); i++) {
      features.add(i);
    }
    Collections.shuffle(features);

    int[] out = new int[features.size()];
    for (int i = 0; i < out.length; i++) {
      out[i] = features.get(i);
    }
    return out;
  }

  private final static int[] sign = new int[]{1, 0, -1};

  public boolean resetWeightVector = true;

  public void learn() {
    double[] regVector = new double[weight.length];
    copy(weight, regVector);//uniform weight distribution

    //this holds the final best model/score
    boolean firstModel = true;
    double[] bestModel = new double[weight.length];
    double bestModelScore = 0;

    PRINTLN("---------------------------");
    PRINTLN("Training starts...");
    PRINTLN("---------------------------");

    for(int r=0;r<nRestart;r++)
    {
      PRINTLN("[+] Random restart #" + (r+1) + "/" + nRestart + "...");

      //initialize weight vector
      if (r > 0 || resetWeightVector) {
        clearWeightVector();
      }


      double startScore = score();

      //local best (within the current restart cycle)
      double bestScoreInRestart = startScore;
      double[] bestWeightInRestart = new double[weight.length];
      copy(weight, bestWeightInRestart);

      //There must be at least one feature increasing whose weight helps
      //while((weight.length>1 && consecutive_fails < weight.length - 1) || (weight.length==1&&consecutive_fails==0))
      int consecutive_fails = 0;
      while(true) {
        if(weight.length == 1) {
          if(consecutive_fails != 0) {
            break;
          }
        } else {
          if(consecutive_fails >= weight.length) {
            break;
          }
        }
        consecutive_fails = 0;

        PRINTLN("\tShuffling features' order and optimizing weight vector...");
        PRINTLN("\t------------------------------");
        PRINTLN(String.format("\t%7s | %8s | %7s", "Feature", "weight", evaluator.measureName));
        //PRINTLN(new int[]{7, 8, 7}, new String[]{"Feature", "weight", scorer.name()});
        PRINTLN("\t------------------------------");

        int[] features = getShuffledFeatures();//contain index of elements in the variable @features

        //Try maximizing each feature individually
        for (int fid : features) {
          //WeightConstraint constraint = evaluator.constraints.get(fid);
          evaluator.beginOptimizing(fid, weight);
          double origWeight = weight[fid];
          double totalStep;
          double bestWeight = 0;
          boolean success = false;//whether or not we succeed in finding a better weight value for the current feature

          for (int dir : sign) {
            weight[fid] = origWeight;//restore the weight to its initial value
            double step = 0.001 * dir;
            // JJFIV - What is this logic doing?
            if (origWeight != 0.0 && Math.abs(step) > 0.5 * Math.abs(origWeight)) {
              step = stepBase * Math.abs(origWeight);
            }
            totalStep = step;
            int numIter = nMaxIteration;
            if(dir == 0) {
              numIter = 1;
              totalStep = -origWeight;
            }
            for (int j = 0; j < numIter; j++) {
              double w = origWeight + totalStep;
              weight[fid] = w;

              // if constraint is not satisfied, don't score this setting:
              //if(!constraint.valid(w))
                //continue;

              double score = score();
              if (regularized) {
                double penalty = slack * getDistance(weight, regVector);
                score -= penalty;
                //PRINTLN("Penalty: " + penalty);
              }
              if (score > bestScoreInRestart)//better than the local best, replace the local best with this model
              {
                bestScoreInRestart = score;
                bestWeight = weight[fid];
                success = true;
                String bw = ((weight[fid] > 0) ? "+" : "") + round(weight[fid], 4);
                String fidSign = dir == 1 ? "+" : "-";
                if(dir == 0) { fidSign = ""; }
                PRINTLN(String.format("\t%7s | %8s | %7s", fidSign+fid, bw, round(bestScoreInRestart, 4)));
              }
              // leave early if we've exceeeded expectations.
              if(score > maxPerformance) {
                success = true;
                break;
              }
              // JJFIV - why only growing here?
              if (j < nMaxIteration - 1) {
                step *= stepScale;
                totalStep += step;
              }
            }
            if (success) {
              break;//no need to search the other direction (e.g. sign = '-')
            }
          }

          // leave early if we've exceeeded expectations.
          if(bestScoreInRestart > maxPerformance) {
            break;
          }

          if (success) {
            weight[fid] = bestWeight;
            consecutive_fails = 0;//since we found a better weight value
            if(normalize) {
              double sum = normalize();
            }
            //scaleCached(sum);
            //System.out.println("Success: w="+bestWeight+" score="+bestScoreInRestart);
            copy(weight, bestWeightInRestart);
          } else {
            consecutive_fails++;
            weight[fid] = origWeight;
          }
        }
        PRINTLN("\t------------------------------");

        //if we haven't made much progress then quit
        if(bestScoreInRestart >= maxPerformance || Math.abs(bestScoreInRestart - startScore) < tolerance) {
          break;
        }
      }

      if(firstModel || bestScoreInRestart > bestModelScore) {
        firstModel = false;
        bestModelScore = bestScoreInRestart;
        copy(bestWeightInRestart, bestModel);
      }
    }

    copy(bestModel, weight);
    double trainingScore = round(score(), 4);
    PRINTLN("---------------------------------");
    PRINTLN("Finished sucessfully.");
    PRINTLN(evaluator.measureName+" on training data: " + trainingScore);
    PRINTLN(Arrays.toString(weight));
    PRINTLN("---------------------------------");
  }

  private double getDistance(double[] w1, double[] w2) {
    assert(w1.length == w2.length);
    double s1 = 0.0;
    double s2 = 0.0;
    for(int i=0;i<w1.length;i++) {
      s1 += Math.abs(w1[i]);
      s2 += Math.abs(w2[i]);
    }
    double dist = 0.0;
    for(int i=0;i<w1.length;i++) {
      double t = w1[i]/s1 - w2[i]/s2;
      dist += t*t;
    }
    return Math.sqrt(dist);
  }

  private double normalize() {
    if(weight.length == 1) return weight[0];
    double sum = 0.0;
    for (double x : weight) sum += Math.abs(x);
    if(sum > 0) {
      for(int j=0;j<weight.length;j++)
        weight[j] /= sum;
      return sum;
    } else {
      clearWeightVector();
      return 1;
    }
  }

  public static double round(double val, int n) {
    //return val;
    int precision = (int) Math.pow(10, n);
    return Math.floor(val * precision + .5)/precision;
  }
}