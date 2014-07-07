package zx.soft.kmeans.cluster.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.Writable;

/**
 * 定义KMeans聚类的特征向量，除了特征向量自己外，还包含一个聚类ID。
 * 
 * @author wanggang
 *
 */
public class VectorWritable implements Writable {

	// 聚类ID
	private int clusterId;
	// 实例（一个聚类点或者一条数据）数量
	private int numInstances;
	// 特征向量
	private Vector<Double> vector;

	public VectorWritable() {
		this(null, 0, 0);
	}

	public VectorWritable(Vector<Double> toWrite, int clusterId) {
		this(toWrite, clusterId, 1);
	}

	public VectorWritable(Vector<Double> toWrite, int clusterId, int numInstances) {
		vector = toWrite;
		this.clusterId = clusterId;
		this.numInstances = numInstances;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(clusterId);
		out.writeInt(numInstances);
		out.writeInt(vector.size());
		for (Double d : vector) {
			out.writeDouble(d.doubleValue());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		clusterId = in.readInt();
		numInstances = in.readInt();
		int length = in.readInt();
		vector = new Vector<Double>(length);
		for (int i = 0; i < length; ++i) {
			vector.add(new Double(in.readDouble()));
		}
	}

	public Vector<Double> getVector() {
		return vector;
	}

	public int getClusterId() {
		return clusterId;
	}

	public int getNumInstances() {
		return numInstances;
	}

}
