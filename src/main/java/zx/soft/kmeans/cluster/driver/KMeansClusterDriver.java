package zx.soft.kmeans.cluster.driver;

import org.apache.hadoop.util.ProgramDriver;

import zx.soft.kmeans.cluster.mapred.KMeansClusterDistribute;
import zx.soft.kmeans.cluster.simple.KMeansClusterSimple;

public class KMeansClusterDriver {

	public static void main(String argv[]) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("kMeansClusterDistribute", KMeansClusterDistribute.class, "分布式KMeans聚类算法");
			pgd.addClass("kMeansClusterSimple", KMeansClusterSimple.class, "简单KMeans聚类算法");
			// Success
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
