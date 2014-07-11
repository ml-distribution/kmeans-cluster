package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 读取特征向量的每行，并输出为VectorWritable类。
 * 初始化阶段，给它们随机赋值一个聚类ID （理想情况下产生一些偶数聚类数）。
 * 该类可以扩展成可以执行Canopy聚类算法，聚类中心存储在DistributedCache中。
 * 
 * @author wanggang
 *
 */
public class KMeansDataInputMapper extends Mapper<LongWritable, Text, IntWritable, VectorWritable> {

	// private int nClusters;
	ArrayList<VectorWritable> centroids;

	@Override
	protected void setup(Context context) throws IOException {
		// 分配随机聚类数
		// nClusters = context.getConfiguration().getInt(KMeansDriver.CLUSTERS, 8);

		// 对于Canopy聚类算法
		centroids = new ArrayList<>();

		/*
		// 需要VectorWritable列表
		Path [] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (files == null || files.length < 1) {
		    throw new IOException("DistributedCache returned an empty file set!");
		}

		// 从DistributedCache读取分片信息
		Configuration conf = context.getConfiguration();
		LocalFileSystem lfs = FileSystem.getLocal(conf);
		for (Path file : files) {
		    SequenceFile.Reader reader = new SequenceFile.Reader(lfs, file, conf);
		    IntWritable key = null;
		    VectorWritable value = null;
		    try {
		        key = (IntWritable)reader.getKeyClass().newInstance();
		        value = (VectorWritable)reader.getValueClass().newInstance();
		    } catch (InstantiationException e) {
		        e.printStackTrace();
		    } catch (IllegalAccessException e) {
		        e.printStackTrace();
		    }
		    while (reader.next(key, value)) {
		        centroids.add(value);
		    }
		    reader.close();
		}
		*/
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String[] elements = value.toString().trim().split("\\s");
		Vector<Double> vector = new Vector<>(elements.length);
		for (int i = 0; i < elements.length; ++i) {
			vector.add(Double.parseDouble(elements[i]));
		}

		// 将数据点输出到非聚类中
		context.write(new IntWritable(-1), new VectorWritable(vector, -1));
	}

}
