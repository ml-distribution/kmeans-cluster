package zx.soft.kmeans.cluster.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 算法用到的参数集合
 * 
 * @author wanggang
 *
 */
public class Params {

	// 记录数
	private int numOfNodes;
	// 每条记录的维度
	private int dim;
	// 聚类数
	private int k;
	// 每个聚类中心点
	private NDimNode[] centroids;
	// 存放每个聚类中的点
	private HashMap<Integer, List<NDimNode>> nodesInCluster;
	// 存放每个聚类的标准差
	private NDimNode[] sigma;
	// 存放每个聚类的先验概率
	private double[] prior;
	// k个Gaussian的最大似然值
	private double logLikelihood;
	// 模型的最小描述长度
	private double minDescLen;
	// 迭代次数
	private int numOfIters;

	@Override
	public String toString() {
		return "Params:[numOfNodes=" + numOfNodes + ",dim=" + dim + ",k=" + k + ",logLikelihood=" + logLikelihood
				+ ",minDescLen=" + minDescLen + ",numOfIters=" + numOfIters + "]";
	}

	public int getNumOfNodes() {
		return numOfNodes;
	}

	public void setNumOfNodes(int numOfNodes) {
		this.numOfNodes = numOfNodes;
	}

	public int getDim() {
		return dim;
	}

	public void setDim(int dim) {
		this.dim = dim;
	}

	public int getK() {
		return k;
	}

	public void setK(int k) {
		this.k = k;
	}

	public NDimNode[] getCentroids() {
		return centroids;
	}

	public void setCentroids(NDimNode[] centroids) {
		this.centroids = centroids;
	}

	public void resetCentroids(int nClusters, int dim) {
		this.centroids = new NDimNode[nClusters];
		for (int i = 0; i < nClusters; i++) {
			this.centroids[i] = new NDimNode(dim);
		}
	}

	public HashMap<Integer, List<NDimNode>> getNodesInCluster() {
		return nodesInCluster;
	}

	public void resetNodesInCluster(int nClusters) {
		this.nodesInCluster = new HashMap<>();
		for (int i = 0; i < nClusters; i++) {
			this.nodesInCluster.put(i, new ArrayList<NDimNode>());
		}
	}

	public void setNodesInCluster(HashMap<Integer, List<NDimNode>> nodesInCluster) {
		this.nodesInCluster = nodesInCluster;
	}

	public NDimNode[] getSigma() {
		return sigma;
	}

	public void setSigma(NDimNode[] sigma) {
		this.sigma = sigma;
	}

	public void resetSigma(int nClusters, int dim) {
		this.sigma = new NDimNode[nClusters];
		for (int i = 0; i < nClusters; i++) {
			this.sigma[i] = new NDimNode(dim);
		}
	}

	public double[] getPrior() {
		return prior;
	}

	public void setPrior(double[] prior) {
		this.prior = prior;
	}

	public double getLogLikelihood() {
		return logLikelihood;
	}

	public void setLogLikelihood(double logLikelihood) {
		this.logLikelihood = logLikelihood;
	}

	public double getMinDescLen() {
		return minDescLen;
	}

	public void setMinDescLen(double minDescLen) {
		this.minDescLen = minDescLen;
	}

	public int getNumOfIters() {
		return numOfIters;
	}

	public void setNumOfIters(int numOfIters) {
		this.numOfIters = numOfIters;
	}

}
