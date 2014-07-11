package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 整合Mapper的输出数据，创建每个类别的局部聚类中心。
 * 以减少网络传输和Reducer端的负载。
 * 
 * @author wanggang
 *
 */
public class KMeansCombiner extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	@Override
	public void reduce(IntWritable clusterId, Iterable<VectorWritable> instances, Context context)
			throws InterruptedException, IOException {

		Vector<Double> partial = new Vector<>();
		int num = 0;

		// 循环向量集合, 并添加到局部聚类中心
		for (VectorWritable instance : instances) {
			Vector<Double> vector = instance.getVector();
			if (vector.size() > 0) {
				partial = add(partial, vector);
				num += 1;
			}
		}

		// 对真实向量和伪向量处理
		if (num > 0) {
			context.write(clusterId, new VectorWritable(partial, clusterId.get(), num));
		} else {
			// 输出虚拟向量
			context.write(clusterId, new VectorWritable(new Vector<Double>(), -1, -1));
		}
	}

	public static Vector<Double> add(Vector<Double> partial, Vector<Double> vector) {
		Vector<Double> result = new Vector<>(vector.size());
		for (int i = 0; i < vector.size(); ++i) {
			if (partial.size() < vector.size()) {
				result.insertElementAt(vector.get(i), i);
			} else {
				result.insertElementAt(partial.get(i) + vector.get(i), i);
			}
		}
		return result;
	}

}
