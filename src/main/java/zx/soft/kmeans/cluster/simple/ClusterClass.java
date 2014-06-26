package zx.soft.kmeans.cluster.simple;

/**
 * The ClusterClass is just a class holding the important cluster properties.
 * @author Patrick van Kouteren 
 *
 */

public class ClusterClass {

	int mean, upperbound, lowerbound;

	public ClusterClass(int m) {
		mean = m;
	}

	public void setBounds(int lb, int ub) {
		lowerbound = lb;
		upperbound = ub;
	}

	public void setMean(int i) {
		mean = i;
	}

	public int getMean() {
		return mean;
	}

	public int getLowerBound() {
		return lowerbound;
	}

	public int getUpperBound() {
		return upperbound;
	}

	public void calculateMean(int[] histogram) {
		int tempMean = 0;
		int counter = 0;
		for (int i = lowerbound; i <= upperbound; i++) {
			counter += histogram[i];
			tempMean += histogram[i] * i;
		}
		mean = tempMean / counter;
	}

}
