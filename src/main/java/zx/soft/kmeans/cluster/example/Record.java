package zx.soft.kmeans.cluster.example;

import java.io.Serializable;

/**
 * 一条记录的数据结构，包括标签和属性。
 * 
 * @author wanggang
 *
 */
public class Record implements Serializable {

	private static final long serialVersionUID = 5956728992101907728L;

	// 标签信息
	private final int label;
	// 属性数据
	private final double[] attributes;

	public Record(int label, double[] attributes) {
		this.label = label;
		this.attributes = attributes;
	}

	public int getLabel() {
		return label;
	}

	public double getAttribute(int index) {
		return attributes[index];
	}

}