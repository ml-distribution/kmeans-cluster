package zx.soft.kmeans.cluster.complex;

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
			KMeansCore.printResults(params);
			if (params.getMinDescLen() < bestMDL) {
				bestModel = k;
				bestMDL = params.getMinDescLen();
			}
		}

		System.out.println("********************************************");
		System.out.println("The most likely model is " + bestModel + " Gaussians");

	}

}
