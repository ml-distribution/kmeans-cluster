package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 从固定数量的本地数据实例中创建一个临时的聚类中心。
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
		for (VectorWritable v : instances) {
			Vector<Double> instance = v.getVector();
			if (instance.size() > 0) {
				partial = add(partial, instance);
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

	public static Vector<Double> add(Vector<Double> partial, Vector<Double> v) {
		Vector<Double> result = new Vector<>(v.size());
		for (int i = 0; i < v.size(); ++i) {
			if (partial.size() < v.size()) {
				result.insertElementAt(v.get(i), i);
			} else {
				result.insertElementAt(partial.get(i).doubleValue() + v.get(i).doubleValue(), i);
			}
		}
		return result;
	}

}
