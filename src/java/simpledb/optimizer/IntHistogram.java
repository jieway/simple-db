package simpledb.optimizer;

import simpledb.execution.Predicate;


/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] heights;
    private int totalTuples;
    private int buckets;
    private int min;
    private int max;
    private int width;
    private int lastBucketWidth;

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
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets         = buckets;
        this.min             = min;
        this.max             = max;
        this.width           = Math.max((max - min + 1) / buckets, 1);
        this.heights         = new int[buckets];
        this.lastBucketWidth = (max - min + 1) - (this.width * (buckets - 1));
        this.totalTuples     = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v > this.max || v < this.min) return;
        int index = (v - this.min) / this.width;
        if (index >= this.buckets) return;
        this.totalTuples++;
        this.heights[index]++;
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
        int bucketIndex =  Math.min((v - this.min) / this.width, this.buckets - 1);
        int bucketWidth = bucketIndex < this.buckets - 1 ? this.width : this.lastBucketWidth;
        double ans = 0;
        if (op == Predicate.Op.EQUALS) {
            ans = estimateEqual(bucketIndex, v, bucketWidth);
        }else if (op == Predicate.Op.NOT_EQUALS) {
            ans = 1.0 - estimateEqual(bucketIndex, v, bucketWidth);
        }else if (op == Predicate.Op.GREATER_THAN) {
            ans = estimateGreater(bucketIndex , v, bucketWidth);
        }else if (op == Predicate.Op.LESS_THAN) {
            ans = 1.0 - estimateGreater(bucketIndex , v, bucketWidth) -
                    estimateEqual(bucketIndex , v, bucketWidth);
        }else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            ans = estimateEqual(bucketIndex, v, bucketWidth) +
                    estimateGreater(bucketIndex, v, bucketWidth);
        }else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            ans = 1 - estimateGreater(bucketIndex , v, bucketWidth);
        }
        return ans;
    }

    public double estimateEqual(int bucketIndex, int predicateValue, int bucketWidth) {
        if (predicateValue < this.min || predicateValue > this.max) return 0;
        // (h/w)/ntups
        return (double) this.heights[bucketIndex] / bucketWidth / this.totalTuples;
    }

    private double estimateGreater(int bucketIndex, int predicateValue, int bucketWidth) {
        if (predicateValue < this.min) return 1.0;
        if (predicateValue > this.max) return 0.0;
        int bucketRight = bucketIndex * this.width + this.min;
        double bucketRatio = (bucketRight - predicateValue) * 1.0 / bucketWidth;
        double result = bucketRatio * (this.heights[bucketIndex] * 1.0 / this.totalTuples);
        int sum = 0;
        for (int i = bucketIndex + 1; i < this.buckets; i++) {
            sum += this.heights[i];
        }
        return (sum * 1.0) / this.totalTuples + result;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        int sum = 0;
        for (int h : heights) {
            sum += h;
        }
        return 1.0 * sum / this.totalTuples;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
