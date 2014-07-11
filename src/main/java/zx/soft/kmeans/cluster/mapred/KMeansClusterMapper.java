package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansClusterMapper extends Mapper<IntWritable, VectorWritable, IntWritable, Text> {

	// 聚类中心集合
	private ArrayList<VectorWritable> centroids;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		Path centroidsPath = new Path(conf.get(KMeansConstant.CENTROIDS));
		centroids = new ArrayList<>(KMeansClusterDistribute.readCentroids(conf, centroidsPath).values());
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

		String attrs = "";
		for (int i = 0; i < vector.size(); i++) {
			attrs += vector.get(i) + " ";
		}
		if (attrs.length() > 1) {
			attrs = attrs.substring(0, attrs.length() - 1);
		}

		// 输出聚类的关系和实例
		context.write(new IntWritable(clusterId), new Text(attrs));

	}

}
