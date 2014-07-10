package zx.soft.kmeans.cluster.complex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * K-Means算法核心类
 * 
 * @author wanggang
 *
 */
public class KMeansCore {

	private static Logger logger = LoggerFactory.getLogger(KMeansCore.class);

	private static final Random RANDOM = new Random();

	public KMeansCore() {
		//
	}

	public Params cluster(NDimNode[] nodes, int k, double epsilon) {

		// 参数初始化
		Params params = init(nodes, k);
		// 终止条件判断值
		double maxDeltaMean = epsilon + 1_000;
		// 上一次迭代的聚类中心
		NDimNode[] oldCentroids = new NDimNode[k];
		// 循环迭代计算，直到聚类中心不变化位置
		while (maxDeltaMean > epsilon) {
			// 前一次迭代的均值
			for (int j = 0; j < k; j++) {
				oldCentroids[j] = new NDimNode(params.getCentroids()[j]);
			}
			// 清空每个聚类
			params.resetNodesInCluster(k);
			// 分类 
			for (int i = 0; i < params.getNumOfNodes(); i++) {
				params.getNodesInCluster().get(classify(params.getCentroids(), nodes[i])).add(nodes[i]);
			}
			// 计算新的聚类均值，即聚类中心
			params.resetCentroids(k, params.getDim());
			params.setCentroids(computeMeans(params.getNodesInCluster(), params.getDim()));
			// 计算最大误差值
			maxDeltaMean = maxDeltaMeans(params.getCentroids(), oldCentroids);
			// 增加迭代次数
			params.setNumOfIters(params.getNumOfIters() + 1);
			// 打印迭代过程
			//			printIteration(params, maxDeltaMean);

		}
		// 计算模型的属性
		modelQuality(params, nodes);
		//		System.out.println(params);
		return params;
	}

	/**
	 * 打印迭代过程
	 */
	public void printIteration(Params params, double maxDeltaMean) {
		System.out.println("聚类中心：");
		for (NDimNode centroid : params.getCentroids()) {
			System.out.print(centroid.getCoordinate(0) + "," + centroid.getCoordinate(1) + "    ");
		}
		System.out.println();
		System.out.println("最大误差值：");
		System.out.println(maxDeltaMean);
		System.out.println("每个聚类数：");
		for (int j = 0; j < params.getK(); j++) {
			System.out.print(params.getNodesInCluster().get(j).size() + "  ");
		}
		System.out.println("\n");
	}

	/**
	 * 初始化各参数，并随机将各点分布到一个聚类中
	 */
	public Params init(NDimNode[] nodes, int k) {
		logger.info("初始化算法的各个参数......");
		Params params = new Params();
		params.setNumOfNodes(nodes.length);
		params.setDim(nodes[0].getDimension());
		params.setK(k);
		HashMap<Integer, List<NDimNode>> nodesInCluster = new HashMap<>();
		for (int i = 0; i < k; i++) {
			nodesInCluster.put(i, new ArrayList<NDimNode>());
		}
		params.setNodesInCluster(nodesInCluster);
		params.setNumOfIters(0);
		NDimNode[] centroids = new NDimNode[k];
		NDimNode[] sigmas = new NDimNode[k];
		double[] priors = new double[k];
		for (int j = 0; j < k; j++) {
			centroids[j] = nodes[RANDOM.nextInt(nodes.length - 1)];
			sigmas[j] = new NDimNode(nodes[0].getDimension());
			priors[j] = 0;
		}
		params.setCentroids(centroids);
		params.setSigma(sigmas);
		params.setPrior(priors);

		return params;
	}

	/**
	 * 计算与当前点最近的一个聚类
	 */
	public int classify(NDimNode[] centroids, NDimNode node) {
		double dist = 0;
		double smallestDist = node.dist(centroids[0]);
		int nearestCate = 0;
		for (int j = 1; j < centroids.length; j++) {
			dist = node.dist(centroids[j]);
			if (dist < smallestDist) {
				smallestDist = dist;
				nearestCate = j;
			}
		}
		return nearestCate;
	}

	/**
	 * 计算模型的属性
	 */
	public void modelQuality(Params params, NDimNode[] nodes) {
		// 计算每个聚类的标准差
		NDimNode[] sigmas = computeDeviation(params.getNodesInCluster(), params.getCentroids(), params.getDim());
		params.setSigma(sigmas);
		// 计算每个聚类的先验概率
		double[] priors = computePriors(params.getNodesInCluster(), params.getNumOfNodes());
		params.setPrior(priors);
		// 计算每个聚类的最大似然值
		double logLikelihood = computeLogLikelihood(nodes, params.getNumOfNodes(), params.getCentroids(),
				params.getSigma(), params.getPrior());
		params.setLogLikelihood(logLikelihood);
		// 计算模型的最小描述长度
		double minDescLen = computeMinDescLen(params.getNodesInCluster(), params.getSigma(), params.getDim(),
				params.getLogLikelihood());
		params.setMinDescLen(minDescLen);
	}

	/**
	 * 计算最小描述长度，前提是每个聚类的最大似然值和先验概率都计算好了
	 */
	public double computeMinDescLen(HashMap<Integer, List<NDimNode>> nodesInCluster, NDimNode[] sigmas, int dim,
			double logLikelihood) {
		double minDescLen = 0;
		for (int j = 0; j < nodesInCluster.size(); j++) {
			for (int i = 0; i < dim; i++) {
				minDescLen -= Math.log(sigmas[j].getCoordinate(i) / Math.sqrt(nodesInCluster.get(j).size()))
						/ Math.log(2);
			}
		}
		return minDescLen - logLikelihood;
	}

	/**
	 * 计算每个聚类的最大似然值，前提是每个聚类的标准差和先验概率都计算好了
	 */
	public double computeLogLikelihood(NDimNode[] nodes, int numOfNodes, NDimNode[] centroids, NDimNode[] sigmas,
			double[] priors) {
		double logLikelihood = 0;
		// 循环每个记录
		for (int i = 0; i < numOfNodes; i++) {
			double temp = 0;
			// 循环每个聚类
			for (int j = 0; j < centroids.length; j++) {
				temp += (nodes[i].normal(centroids[j], sigmas[j]) * priors[j]);
			}
			logLikelihood += Math.log(temp) / Math.log(2);
		}
		return logLikelihood;
	}

	/**
	 * 计算每个聚类的先验概率，即k个Gaussians的先验概率
	 */
	public double[] computePriors(HashMap<Integer, List<NDimNode>> nodesInCluster, int numOfNodes) {
		double[] priors = new double[nodesInCluster.size()];
		for (int j = 0; j < nodesInCluster.size(); j++) {
			priors[j] = ((double) nodesInCluster.get(j).size()) / numOfNodes;
		}
		return priors;
	}

	/**
	 * 计算每个聚类的标准差，即k个Gaussians的标准差
	 */
	public NDimNode[] computeDeviation(HashMap<Integer, List<NDimNode>> nodesInCluster, NDimNode[] centroids, int dim) {
		// 标准差
		NDimNode[] sigmas = new NDimNode[centroids.length];
		for (int i = 0; i < nodesInCluster.size(); i++) {
			sigmas[i] = new NDimNode(dim);
		}
		// 循环每个聚类
		for (int j = 0; j < centroids.length; j++) {
			for (int i = 0; i < nodesInCluster.get(j).size(); i++) {
				NDimNode temp = new NDimNode(nodesInCluster.get(j).get(i));
				// (x[i]-mu[j])^2
				temp.subtract(centroids[j]);
				temp.pow(2.0);
				// 乘以x[i]在聚类j中出现的概率
				temp.multiply(1.0 / nodesInCluster.get(j).size());
				// 增加(x[i]-mu[j])^2 * p(x[i])
				sigmas[j].add(temp);
			}
			// 计算标准差，所以开根号
			sigmas[j].pow(1.0 / 2);
		}
		return sigmas;
	}

	/**
	 * 计算两次迭代中，所有聚类中改变最大的那个差值
	 */
	public double maxDeltaMeans(NDimNode[] centroids, NDimNode[] oldCentroids) {
		oldCentroids[0].subtract(centroids[0]);
		double maxDelta = oldCentroids[0].norm();
		for (int j = 1; j < centroids.length; j++) {
			oldCentroids[j].subtract(centroids[j]);
			maxDelta = Math.max(oldCentroids[j].norm(), maxDelta);
		}
		return maxDelta;
	}

	/**
	 * 计算每个聚类的中心
	 */
	public NDimNode[] computeMeans(HashMap<Integer, List<NDimNode>> nodesInCluster, int dim) {
		NDimNode[] centroids = new NDimNode[nodesInCluster.size()];
		for (int i = 0; i < nodesInCluster.size(); i++) {
			centroids[i] = new NDimNode(dim);
		}
		// 计算均值
		for (int j = 0; j < nodesInCluster.size(); j++) {
			for (int i = 0; i < nodesInCluster.get(j).size(); i++) {
				centroids[j].add(nodesInCluster.get(j).get(i));
			}
			centroids[j].multiply(1.0 / nodesInCluster.get(j).size());
		}
		return centroids;
	}

}
