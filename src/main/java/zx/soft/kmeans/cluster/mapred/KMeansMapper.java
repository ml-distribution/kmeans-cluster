package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Reads an instance from HDFS, computes the Euclidean distance between it
 * and each cluster centroid (from the DistributedCache), and assigns it to 
 * the nearest one. If this centroid is different from the instance's previous
 * cluster assignment, the CONVERGED counter is updated.
 * 
 * @author wgybzb
 *
 */
public class KMeansMapper extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	private ArrayList<VectorWritable> centroids;
	private int nClusters;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		Path centroidsPath = new Path(conf.get(KMeansClusterDistribute.CENTROIDS));
		centroids = new ArrayList<VectorWritable>(KMeansClusterDistribute.readCentroids(conf, centroidsPath).values());
		nClusters = conf.getInt(KMeansClusterDistribute.CLUSTERS, centroids.size());
	}

	@Override
	public void map(IntWritable previousId, VectorWritable instance, Context context) throws InterruptedException,
			IOException {

		// 计算当前的instance距离哪一个中心点最近
		Vector<Double> v = instance.get();
		double distance = Double.MAX_VALUE;
		int clusterId = -1;
		for (VectorWritable centroid : centroids) {
			// 计算Euclidean距离(L2 norm)
			Vector<Double> c = centroid.get();
			double squaredSum = 0.0;
			for (int i = 0; i < c.size(); ++i) {
				double di = c.get(i).doubleValue() - v.get(i).doubleValue();
				squaredSum += (di * di);
			}
			squaredSum = Math.sqrt(squaredSum);
			if (squaredSum < distance) {
				distance = squaredSum;
				clusterId = centroid.getClusterId();
			}
		}

		// 输出聚类的关系和实例
		context.write(new IntWritable(clusterId), instance);

		// 输出一串伪向量.
		VectorWritable dummy = new VectorWritable(new Vector<Double>(), -1, -1);
		for (int i = 1; i <= nClusters; ++i) {
			if (i != clusterId) {
				context.write(new IntWritable(i), dummy);
			}
		}
	}

}