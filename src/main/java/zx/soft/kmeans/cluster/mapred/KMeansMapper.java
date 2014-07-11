package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 从HDFS中读取一个实例，并计算它与每个聚类中心的距离（Euclidean距离），
 * 将最近的那个作为其中心；如果该中心与之前的不一样，则更新CONVERGED计数器。
 * 其中，聚类中新数据从DistributedCache中读取。
 * 
 * @author wanggang
 *
 */
public class KMeansMapper extends Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	// 聚类中心集合
	private ArrayList<VectorWritable> centroids;
	// 聚类中心数
	private int nClusters;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		Path centroidsPath = new Path(conf.get(KMeansConstant.CENTROIDS));
		centroids = new ArrayList<>(KMeansClusterDistribute.readCentroids(conf, centroidsPath).values());
		nClusters = conf.getInt(KMeansConstant.CLUSTERS, centroids.size());
	}

	@Override
	public void map(IntWritable previousId, VectorWritable instance, Context context) throws InterruptedException,
			IOException {

		// 计算当前的实例与聚类中心集合中最近的那个中心
		Vector<Double> vector = instance.getVector();
		double distance = Double.MAX_VALUE;
		int clusterId = -1;
		for (VectorWritable centroid : centroids) {
			// 计算Euclidean距离(L2 norm)
			Vector<Double> temp = centroid.getVector();
			double squaredSum = 0.0;
			for (int i = 0; i < temp.size(); ++i) {
				squaredSum += Math.pow(temp.get(i).doubleValue() - vector.get(i).doubleValue(), 2.0);
			}
			squaredSum = Math.sqrt(squaredSum);
			if (squaredSum < distance) {
				distance = squaredSum;
				clusterId = centroid.getClusterId();
			}
		}

		// 输出聚类的关系和实例
		context.write(new IntWritable(clusterId), instance);

		// 输出一串虚拟向量
		VectorWritable dummy = new VectorWritable(new Vector<Double>(), -1, -1);
		for (int i = 1; i <= nClusters; ++i) {
			if (i != clusterId) {
				context.write(new IntWritable(i), dummy);
			}
		}
	}

}