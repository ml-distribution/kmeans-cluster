package zx.soft.kmeans.cluster.image;

/**
 * 聚类属性
 * 
 * @author wanggang
 *
 */
public class Cluster {

	int mean, upperbound, lowerbound;

	public Cluster(int m) {
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
