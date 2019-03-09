package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int bucketSize;
    private int min;
    private int max;
    private int[] buckets;
    private int numTuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param numBuckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int numBuckets, int min, int max) {
        this.min = min;
        this.max = max;
        this.buckets = new int[numBuckets];
        this.bucketSize = (int) Math.ceil((max - min + 1) / (numBuckets * 1.0));
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        numTuples++;
        buckets[bucketIndex(v)]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int bucketIndex = bucketIndex(v);
        double selectivity = 0F;

        switch (op) {
            case EQUALS:
                selectivity = (buckets[bucketIndex] / (bucketSize * 1.0)) / (numTuples * 1.0);
                break;
            case NOT_EQUALS:
                selectivity = (numTuples - buckets[bucketIndex] / (bucketSize * 1.0)) / (numTuples * 1.0);
                break;
            case GREATER_THAN:
                for (int i = bucketIndex; i < buckets.length; i++) {
                    double bucketFraction = bucketHeight(i) / (numTuples * 1.0);
                    double bucketPartition = 1;
                    if (i == bucketIndex) {
                        bucketPartition = bucketMaxValue(bucketIndex) - v / (bucketWidth(bucketIndex) * 1.0);

                    }
                    selectivity += (bucketFraction * bucketPartition);
                }
                break;
            case GREATER_THAN_OR_EQ:
                for (int i = bucketIndex; i < buckets.length; i++) {
                    double bucketFraction = bucketHeight(i) / (numTuples * 1.0);
                    double bucketPartition = 1;
                    if (i == bucketIndex) {
                        bucketPartition = bucketMaxValue(bucketIndex) - v + 1 / (bucketWidth(bucketIndex) * 1.0);

                    }
                    selectivity += (bucketFraction * bucketPartition);
                }
                break;
            case LESS_THAN:
                for (int i = bucketIndex; i >= 0; i--) {
                    double bucketFraction = bucketHeight(i) / (numTuples * 1.0);
                    double bucketPartition = 1;
                    if (i == bucketIndex) {
                        bucketPartition = v - bucketMinValue(i) / (bucketWidth(bucketIndex) * 1.0);

                    }
                    selectivity += (bucketFraction * bucketPartition);
                }
                break;
            case LESS_THAN_OR_EQ:
                for (int i = bucketIndex; i >= 0; i--) {
                    double bucketFraction = bucketHeight(i) / (numTuples * 1.0);
                    double bucketPartition = 1;
                    if (i == bucketIndex) {
                        bucketPartition = v - bucketMinValue(i) + 1 / (bucketWidth(bucketIndex) * 1.0);

                    }
                    selectivity += (bucketFraction * bucketPartition);
                }
                break;
            default:
        }

        return selectivity;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buckets.length; i++) {
            sb.append(bucketMinValue(i)).append("-").append(bucketMaxValue(i)).append(": ").append(buckets[i]);
            if (i < buckets.length - 1) sb.append(", ");
        }

        return sb.toString();
    }

    private int bucketIndex(int v) {
        if (v < min) {
            return - 1;
        } else if (v > max) {
            return buckets.length;
        } else {
            return (int) Math.floor((v - min) / (bucketSize * 1.0));
        }
    }

    private int bucketMinValue(int index) {
        return min + (index * bucketSize);
    }

    private int bucketMaxValue(int index) {
        return min + ((index + 1) * bucketSize) - 1;
    }

    private int bucketWidth(int index) {
        return bucketMaxValue(index) - bucketMinValue(index) + 1;
    }

    private int bucketHeight(int index) {
        return (index >= 0 && index < buckets.length) ? buckets[index] : 0;
    }
}

