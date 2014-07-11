package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.Random;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 初始化中心数据，并输出为VectorWritable类。
 * 
 * @author wanggang
 *
 */
public class KMeansCentroidInputMapper extends Mapper<LongWritable, Text, IntWritable, VectorWritable> {

	private static final Random RANDOM = new Random();

	private int nClusters;

	@Override
	protected void setup(Context context) throws IOException {
		nClusters = context.getConfiguration().getInt(KMeansConstant.CLUSTERS, 8);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		// 提取特征向量和聚类ID
		String[] elements = value.toString().trim().split("\\s");

		Vector<Double> vector = new Vector<>(elements.length);
		int clusterId = RANDOM.nextInt(nClusters) + 1;
		for (int i = 0; i < elements.length; ++i) {
			vector.add(Double.parseDouble(elements[i]));
		}

		context.write(new IntWritable(clusterId), new VectorWritable(vector, clusterId, 0));
	}

}
