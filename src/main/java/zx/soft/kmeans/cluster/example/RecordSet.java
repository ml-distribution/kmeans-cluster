package zx.soft.kmeans.cluster.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.StringTokenizer;

/**
 * 读取txt文件，并创建一个数据集合。
 * 
 * @author wanggang
 *
 */
public class RecordSet implements Serializable {

	private static final long serialVersionUID = 6903619406397128860L;

	private int size;
	private int attributeNum;
	Record records[];

	public RecordSet(String filename) {
		try (BufferedReader br = new BufferedReader(new FileReader(filename));) {
			// 文件的第一行包含数据集大小，以及每条记录的属性数
			String newLine = br.readLine();

			StringTokenizer st = new StringTokenizer(newLine);
			size = Integer.parseInt(st.nextToken());
			attributeNum = Integer.parseInt(st.nextToken());
			records = new Record[size];

			double attributes[] = new double[attributeNum];

			// 读取每条记录
			for (int k = 0; k < size; k++) {
				newLine = br.readLine();
				st = new StringTokenizer(newLine);
				for (int i = 0; i < attributeNum; i++) {
					attributes[i] = Double.parseDouble(st.nextToken());
				}
				records[k] = new Record(0, attributes);
			}
		} catch (IOException e) {
			System.err.println(e.toString());
			throw new RuntimeException(e);
		}
	}

	public int size() {
		return size;
	}

	public Record getRecord(int index) {
		return records[index];
	}

	public int getRecordLabel(int index) {
		return records[index].getLabel();
	}

	public int getAttributeNum() {
		return attributeNum;
	}

	public double getAttribute(int i, int j) {
		return records[i].getAttribute(j);
	}

}