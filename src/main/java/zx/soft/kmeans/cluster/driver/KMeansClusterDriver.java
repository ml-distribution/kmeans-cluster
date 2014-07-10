package zx.soft.kmeans.cluster.driver;

import org.apache.hadoop.util.ProgramDriver;

import zx.soft.kmeans.cluster.complex.KMeansCore;
import zx.soft.kmeans.cluster.mapred.KMeansClusterDistribute;

public class KMeansClusterDriver {

	public static void main(String argv[]) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("kMeansClusterDistribute", KMeansClusterDistribute.class, "分布式KMeans聚类算法");
			pgd.addClass("kMeansCore", KMeansCore.class, "简单KMeans聚类算法");
			pgd.driver(argv);
			// Success
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

}
