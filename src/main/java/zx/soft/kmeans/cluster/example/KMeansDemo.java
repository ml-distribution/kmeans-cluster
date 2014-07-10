package zx.soft.kmeans.cluster.example;

import java.io.IOException;

public class KMeansDemo {

	/**
	 * 主函数
	 */
	public static void main(String[] args) throws IOException {

		String inputFile = "sample/test-data";

		/*
		 * 初始化
		 */
		NodesSet nodesSet = new NodesSet(inputFile);
		int maxk = 6;
		int numNodes = nodesSet.getSize();
		int dim = nodesSet.getAttributeNum();
		NDimNode[] nodes = new NDimNode[numNodes];
		for (int i = 0; i < numNodes; i++) {
			nodes[i] = new NDimNode(dim);
			for (int d = 0; d < dim; d++) {
				nodes[i].setCoordinate(d, nodesSet.getRecords()[i].getCoordinate(d));
			}
		}

		// 终止条件
		double epsilon = 0.001;

		KMeansCore kmeans = new KMeansCore();
		int bestModel = 0;
		double bestMDL = 1_000_000_000;
		//循环计算，找出最好的k值
		for (int k = 1; k <= maxk; k++) {
			System.out.println("********************************************");
			Params params = kmeans.cluster(nodes, k, epsilon);
			KMeansDemo.printResults(params);
			if (params.getMinDescLen() < bestMDL) {
				bestModel = k;
				bestMDL = params.getMinDescLen();
			}
		}

		System.out.println("********************************************");
		System.out.println("The most likely model is " + bestModel + " Gaussians");

	}

	/**
	 * 打印每次迭代过程中的参数值
	 */
	public static void printResults(Params params) {
		System.out.println("Trying " + params.getK() + " clusters...");
		System.out.println("Converged after " + params.getNumOfIters() + " iterations");
		for (int j = 0; j < params.getK(); j++) {
			System.out.println();
			System.out.println("Gaussian no. " + (j + 1));
			System.out.println("---------------");
			System.out.println("mean " + params.getCentroids()[j]);
			System.out.println("sigma " + params.getSigma()[j]);
			System.out.println("prior " + params.getPrior()[j]);
		}
		System.out.println();
		System.out.println("Model quality:");
		System.out.println("Log-Likelihood " + params.getLogLikelihood());
		System.out.println("MdL " + params.getMinDescLen());
	}

}
