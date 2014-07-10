package zx.soft.kmeans.cluster.mapred;

/**
 * 常量类
 * 
 * @author wanggang
 *
 */
public class KMeansConstant {

	// 收敛计数器，如果CONVERGED对应的计数值不为0,说明还没有达到迭代收敛误差
	public static enum Counter {
		CONVERGED
	}

	// 迭代
	public static final String ITERATIONS = "KMeansSimple.cluster.converged";

	// 聚类
	public static final String CLUSTERS = "KMeansSimple.cluster.clusters";

	// 迭代终止误差
	public static final String TOLERANCE = "KMeansSimple.cluster.tolerance";

	// 聚类中心
	public static final String CENTROIDS = "KMeansSimple.cluster.centroids";

}
