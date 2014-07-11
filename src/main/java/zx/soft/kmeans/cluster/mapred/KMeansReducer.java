package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 依据Hadoop使用的Combiners数量，读取实例和局部聚类中心（通过numInstances区分）的混合数据。
 * 使用该信息计算每个聚类的最终中心集合。
 * 
 * @author wanggang
 *
 */
public class KMeansReducer extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	private HashMap<Integer, VectorWritable> centroids;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		Path centroidsPath = new Path(conf.get(KMeansConstant.CENTROIDS));
		centroids = KMeansClusterDistribute.readCentroids(conf, centroidsPath);
	}

	@Override
	public void reduce(IntWritable clusterId, Iterable<VectorWritable> instances, Context context) throws IOException,
			InterruptedException {

		Vector<Double> centroid = new Vector<>();
		int num = 0;
		for (VectorWritable instance : instances) {
			Vector<Double> vector = instance.getVector();
			if (vector.size() > 0) {
				centroid = KMeansCombiner.add(centroid, vector);
				num += instance.getNumInstances();
			}
		}

		// 空的中心
		if (num == 0) {
			// 使用上一次迭代的聚类中心
			context.write(clusterId, centroids.get(clusterId.get()));
		} else {
			// 计算平均值，并记录当前值与上一次迭代的差的绝对值的总和
			double residual = 0.0;
			Vector<Double> previous = centroids.get(clusterId.get()).getVector();
			for (int i = 0; i < centroid.size(); ++i) {
				double value = centroid.get(i) / num;
				centroid.set(i, value);
				residual += Math.abs(value - previous.get(i));
			}

			// 判断中心是否改变
			float tolerance = context.getConfiguration().getFloat(KMeansConstant.TOLERANCE, 0.000001F);
			if (residual > tolerance) {
				context.getCounter(KMeansConstant.Counter.CONVERGED).increment(1);
			}

			// 输出新的中心
			context.write(clusterId, new VectorWritable(centroid, clusterId.get(), num));
		}
	}

}