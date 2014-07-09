package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 读取中心数据，并输出为VectorWritable类。
 * 
 * @author wanggang
 *
 */
public class KMeansCentroidInputMapper extends Mapper<LongWritable, Text, IntWritable, VectorWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		// 提取特征向量和聚类ID
		String[] elements = value.toString().trim().split("\\s");

		Vector<Double> out = new Vector<>();
		int clusterId = Integer.parseInt(elements[0]);
		for (int i = 2; i < elements.length; ++i) {
			out.add(Double.parseDouble(elements[i]));
		}

		context.write(new IntWritable(clusterId), new VectorWritable(out, clusterId, 0));
	}

}
