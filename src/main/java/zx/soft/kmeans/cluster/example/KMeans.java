package zx.soft.kmeans.cluster.example;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * K-Means算法实现类
 * 
 * @author wanggang
 *
 */
public class KMeans {

	private static Logger logger = LoggerFactory.getLogger(KMeans.class);

	// 记录数
	private int numOfRecords;
	// 每条记录的维度
	private int dim;
	// 聚类数
	private int k;
	// 每个聚类中心点
	private NDimNode[] centroids;
	// 存放每个聚类中的点
	private Vector<NDimNode>[] recordsInCluster;
	// 存放每个聚类的标准差
	private NDimNode[] sigma;
	// 存放每个聚类的prior
	private double[] prior;
	// k个Gaussian的最大似然值
	private double logLikelihood;
	// 模型的最小描述长度
	private double MDL;
	// 迭代次数
	private int numIterations;

	public KMeans() {
		//
	}

	/**
	 * 初始化各参数，并随机将各点分布到一个聚类中
	 */
	private void init(NDimNode[] records, int k) {
		this.numOfRecords = records.length;
		this.dim = records[0].getDimension();
		this.k = k;
		this.centroids = new NDimNode[k];
		this.recordsInCluster = new Vector[k];
		this.numIterations = 0;
		this.sigma = new NDimNode[k];
		this.prior = new double[k];

		for (int j = 0; j < k; j++) {
			centroids[j] = records[(int) (Math.random() * (numOfRecords - 1))];
			sigma[j] = new NDimNode(dim);
			prior[j] = 0;
		}
	}

	public void run(NDimNode[] records, int k, double epsilon) {

		double maxDeltaMeans = epsilon + 1;
		NDimNode[] oldMeans = new NDimNode[k];
		// 参数初始化
		init(records, k);
		// 迭代计算，直到recordsInCluster不变化位置
		while (maxDeltaMeans > epsilon) {
			// remember old values of the each mean
			for (int j = 0; j < k; j++) {
				oldMeans[j] = new NDimNode(centroids[j]);
			}
			// classify each instance x[i] to its nearest class
			// first we need to clear the class array since we are reclassifying
			for (int j = 0; j < k; j++) {
				recordsInCluster[j] = new Vector<NDimNode>(); // could use clear but then have to init...
			}

			for (int i = 0; i < numOfRecords; i++) {
				classify(records[i]);
			}
			// recompute each mean
			computeMeans();
			// compute the largest change in mu[j]
			maxDeltaMeans = maxDeltaMeans(oldMeans);
			numIterations++;
		}
		// now we find the quality of the model
		modelQuality(records);
	}

	/**
	 * Find the quality of the model
	 **/
	private void modelQuality(NDimNode[] records) {
		// compute the standard deviation of each cluster
		computeDeviation();
		// compute the prior of each cluster
		computePriors();
		// compute the log likelihood of each cluster
		computeLogLikelihood(records);
		// find the minimum description length of the model
		computeMDL();
	}

	/**
	 * 计算与当前点最近的一个聚类
	 */
	private void classify(NDimNode record) {
		double dist = 0;
		double smallestDist;
		int nearestCate;

		smallestDist = record.dist(centroids[0]);
		nearestCate = 0;

		for (int j = 1; j < k; j++) {
			dist = record.dist(centroids[j]);
			if (dist < smallestDist) {
				smallestDist = dist;
				nearestCate = j;
			}
		}

		recordsInCluster[nearestCate].add(record);
	}

	/**
	 * 计算每个聚类的中心
	 */
	private void computeMeans() {
		int numRecords;
		NDimNode instance;

		// 置0
		for (int j = 0; j < k; j++)
			centroids[j].setToOrigin();

		// 计算均值
		for (int j = 0; j < k; j++) {
			numRecords = recordsInCluster[j].size();
			for (int i = 0; i < numRecords; i++) {
				instance = recordsInCluster[j].get(i);
				centroids[j].add(instance);
			}
			centroids[j].multiply(1.0 / numRecords);
		}
	}

	/**
	 * 计算两次迭代中，所有聚类中改变最大的那个差值
	 */
	private double maxDeltaMeans(NDimNode[] oldMeans) {
		double delta;
		oldMeans[0].subtract(centroids[0]);
		double maxDelta = oldMeans[0].max();
		for (int j = 1; j < k; j++) {
			oldMeans[j].subtract(centroids[j]);
			delta = oldMeans[j].max();
			if (delta > maxDelta)
				maxDelta = delta;
		}
		return maxDelta;
	}

	/**
	 * 打印每次迭代过程中的参数值
	 */
	public void printResults() {
		System.out.println("********************************************");
		System.out.println("Trying " + k + " clusters...");
		System.out.println("Converged after " + numIterations + " iterations");
		for (int j = 0; j < k; j++) {
			System.out.println();
			System.out.println("Gaussian no. " + (j + 1));
			System.out.println("---------------");
			System.out.println("mean " + centroids[j]);
			System.out.println("sigma " + sigma[j]);
			System.out.println("prior " + prior[j]);
		}
		System.out.println();
		System.out.println("Model quality:");
		System.out.println("Log-Likelihood " + logLikelihood);
		System.out.println("MdL " + MDL);
	}

	/**
	 * Write into a file the k Gaussians (one for each column)
	 * Only works for 2 dimensional points
	 **/
	public void writeFile(FileWriter out) throws IOException {
		//save the MDL of this model
		out.write(MDL + "\r");
		for (int j = 0; j < k; j++) {
			out.write("Gaussian" + (j + 1) + " ");
		}
		out.write("\r");
		// save the means of each Gaussian
		for (int j = 0; j < k; j++) {
			out.write(centroids[j] + " ");
		}
		out.write("\r");
		// save the points in each Gaussian for each column
		int numInstances = 0;
		for (int i = 0; i < numOfRecords; i++) {
			for (int j = 0; j < k; j++) {
				numInstances = recordsInCluster[j].size();
				if (i < numInstances)
					out.write(recordsInCluster[j].get(i) + " ");
				else
					out.write("" + " " + "" + " ");
			}
			out.write("\r");
		}
	}

	/**
	 * Compute the standard deviation of the k Gaussians
	 **/
	private void computeDeviation() {
		int numInstances; // number of instances in each class w[j]
		NDimNode instance;
		NDimNode temp;

		// set the standard deviation to zero
		for (int j = 0; j < k; j++)
			sigma[j].setToOrigin();

		// for each cluster j...
		for (int j = 0; j < k; j++) {
			numInstances = recordsInCluster[j].size();
			for (int i = 0; i < numInstances; i++) {
				instance = (recordsInCluster[j].get(i));
				temp = new NDimNode(instance);
				temp.subtract(centroids[j]);
				temp.pow(2.0); // (x[i]-mu[j])^2
				temp.multiply(1.0 / numInstances); // multiply by proba of having x[i] in cluster j
				sigma[j].add(temp); // sum i (x[i]-mu[j])^2 * p(x[i])
			}
			sigma[j].pow(1.0 / 2); // because we want the standard deviation
		}
	}

	/**
	 * Compute the priors of the k Gaussians
	 **/
	private void computePriors() {
		double numInstances; // number of instances in each class w[j]
		for (int j = 0; j < k; j++) {
			numInstances = recordsInCluster[j].size() * (1.0);
			prior[j] = numInstances / numOfRecords;
		}
	}

	/**
	 * Assume the standard deviations and priors of each cluster have been computed
	 **/
	private void computeLogLikelihood(NDimNode[] x) {
		double temp1 = 0;
		double temp2 = 0;
		//		NDimNode variance;
		double ln2 = Math.log(2);
		// for each instance x
		for (int i = 0; i < numOfRecords; i++) {
			// for each cluster j
			temp1 = 0;
			for (int j = 0; j < k; j++) {
				temp1 = temp1 + (x[i].normal(centroids[j], sigma[j]) * prior[j]);
			}
			temp2 = temp2 + Math.log(temp1) / ln2;
		}
		logLikelihood = temp2;
	}

	/**
	 * Assume the log likelihood and priors have been computed
	 **/
	private void computeMDL() {
		double temp = 0;
		double numInstances;
		double ln2 = Math.log(2);
		for (int j = 0; j < k; j++) {
			numInstances = recordsInCluster[j].size();
			for (int i = 0; i < dim; i++) {
				temp = temp - Math.log(sigma[j].getCoordinate(i) / Math.sqrt(numInstances)) / ln2;
			}
		}
		MDL = temp - logLikelihood;
	}

	public double getMDL() {
		return MDL;
	}

	/**
	 * Takes the data filename of instances to classify into a number of cluster k
	 * Runs the k-means algorithm with 1 to maxk clusters
	 **/
	public static void main(String[] args) throws IOException {

		String inputFile = "sample/test-data";
		RecordSet dataFile = new RecordSet(inputFile);
		int maxk = 4;

		// make the instance array
		int numInstances = dataFile.size();
		int d = dataFile.getAttributeNum();
		NDimNode[] x = new NDimNode[numInstances];
		Record instance;
		for (int i = 0; i < numInstances; i++) {
			instance = dataFile.getRecord(i);
			x[i] = new NDimNode(d);
			for (int k = 0; k < d; k++) {
				x[i].setCoordinate(k, instance.getAttribute(k));
			}
		}

		// used to write into files
		File outputFile = new File("testClustering.txt");
		FileWriter out = new FileWriter(outputFile);

		// choose threshold for when to stop
		double epsilon = 0.01; // make it data driven (right now absolute..)

		// run the k-means algorithm on this datafile with a max number of clusters as maxk
		KMeans algorithm = new KMeans();
		// Try with k clusters....till maxk clusters
		int bestModel = 1;
		double bestMDL = 1000000000; // change this
		for (int k = 1; k <= maxk; k++) {
			algorithm.run(x, k, epsilon);
			algorithm.printResults();
			if (algorithm.getMDL() < bestMDL) {
				bestModel = k;
				bestMDL = algorithm.getMDL();
			}
		}
		// Report the best model
		System.out.println("********************************************");
		System.out.println("The most likely model is " + bestModel + " Gaussians");

		// write into file testClustering.txt the most likely model
		algorithm.run(x, bestModel, epsilon);
		algorithm.writeFile(out);
		out.close();
	}
}