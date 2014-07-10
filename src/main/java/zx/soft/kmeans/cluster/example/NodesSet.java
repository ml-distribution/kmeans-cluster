package zx.soft.kmeans.cluster.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * 读取txt文件，并创建一个数据集合。
 * 
 * @author wanggang
 *
 */
public class NodesSet {

	private int size = 0;
	private int attributeNum = 0;
	NDimNode[] nodes;

	public NodesSet(String filename) {
		init(filename);
		nodes = new NDimNode[size];
		for (int i = 0; i < size; i++) {
			nodes[i] = new NDimNode();
		}
		String str;
		String[] strs;
		int count = 0;
		try (BufferedReader br = new BufferedReader(new FileReader(filename));) {
			while ((str = br.readLine()) != null) {
				strs = str.split("\\s");
				double[] coordinates = new double[attributeNum];
				for (int i = 0; i < attributeNum; i++) {
					coordinates[i] = Double.parseDouble(strs[i]);
				}
				nodes[count++].setCoordinates(coordinates);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void init(String filename) {
		String str;
		try (BufferedReader br = new BufferedReader(new FileReader(filename));) {
			while ((str = br.readLine()) != null) {
				size++;
				attributeNum = str.split("\\s").length;
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public int getAttributeNum() {
		return attributeNum;
	}

	public void setAttributeNum(int attributeNum) {
		this.attributeNum = attributeNum;
	}

	public NDimNode[] getRecords() {
		return nodes;
	}

	public void setRecords(NDimNode[] records) {
		this.nodes = records;
	}

}