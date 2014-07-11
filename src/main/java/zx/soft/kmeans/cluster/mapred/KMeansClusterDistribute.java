package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.kmeans.cluster.utils.HDFSUtils;

/**
 * K-Means聚类算法主类
 * 
 * @author wanggang
 *
 */
public class KMeansClusterDistribute extends Configured implements Tool {

	private static Logger logger = LoggerFactory.getLogger(KMeansClusterDistribute.class);

	/**
	 * 从HDFS中读取序列化形式的中心数据
	 */
	public static HashMap<Integer, VectorWritable> readCentroids(Configuration conf, Path path) throws IOException {
		logger.info("开始读取中心数据......");
		HashMap<Integer, VectorWritable> centroids = new HashMap<>();
		FileSystem fs = FileSystem.get(path.toUri(), conf);
		FileStatus[] list = fs.globStatus(new Path(path, "part-*"));
		for (FileStatus status : list) {
			SequenceFile.Reader reader = null;
			try {
				reader = new SequenceFile.Reader(fs, status.getPath(), conf);
				IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				VectorWritable value = (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				while (reader.next(key, value)) {
					centroids.put(key.get(),
							new VectorWritable(value.getVector(), value.getClusterId(), value.getNumInstances()));
				}
			} finally {
				IOUtils.closeStream(reader);
			}
		}
		logger.info("读取中心数据结束......");
		return centroids;
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();

		// 读取命令行参数
		Path dataInput = new Path(conf.get("input"));
		Path output = new Path(conf.get("output"));
		int nClusters = conf.getInt("clusters", 8);
		int nReducers = conf.getInt("reducers", 8);
		float tolerance = conf.getFloat("tolerance", 1e-6F);

		/**
		 * 作业1: 输入数据读取，并初始化聚类中心。
		 * 
		 * Canopy聚类算法用于解决随机初始化不均匀问题。
		 */
		Configuration dataConf = new Configuration();
		dataConf.setInt(KMeansConstant.CLUSTERS, nClusters);
		Job inputDataJob = new Job(dataConf);
		inputDataJob.setJobName("KMeans-Cluster-Data-Input/Canopy");
		inputDataJob.setJarByClass(KMeansClusterDistribute.class);
		Path data = new Path(output, "formattedData");
		HDFSUtils.delete(dataConf, data);

		inputDataJob.setInputFormatClass(TextInputFormat.class);
		inputDataJob.setOutputFormatClass(SequenceFileOutputFormat.class);

		inputDataJob.setMapperClass(KMeansDataInputMapper.class);
		// 没有Combiner和Reducer

		inputDataJob.setMapOutputKeyClass(IntWritable.class);
		inputDataJob.setMapOutputValueClass(VectorWritable.class);
		inputDataJob.setOutputKeyClass(IntWritable.class);
		inputDataJob.setOutputValueClass(VectorWritable.class);

		FileInputFormat.addInputPath(inputDataJob, dataInput);
		FileOutputFormat.setOutputPath(inputDataJob, data);
		inputDataJob.setNumReduceTasks(0);

		if (!inputDataJob.waitForCompletion(true)) {
			System.err.println("Data input job failed!");
			System.exit(1);
		}

		/**
		 * 作业2: 中心数据读取。
		 * 
		 * 如果使用Canopy聚类算法的话，无需执行该作业。
		 */
		Job centroidInputJob = new Job(dataConf);
		centroidInputJob.setJobName("KMeans-Cluster-Centroid-Init");
		centroidInputJob.setJarByClass(KMeansClusterDistribute.class);
		Path centroidsPath = new Path(output, "centroids_0");
		HDFSUtils.delete(dataConf, centroidsPath);

		centroidInputJob.setInputFormatClass(TextInputFormat.class);
		centroidInputJob.setOutputFormatClass(SequenceFileOutputFormat.class);

		centroidInputJob.setMapperClass(KMeansCentroidInputMapper.class);
		// 没有Combiner和Reducer

		centroidInputJob.setMapOutputKeyClass(IntWritable.class);
		centroidInputJob.setMapOutputValueClass(VectorWritable.class);
		centroidInputJob.setOutputKeyClass(IntWritable.class);
		centroidInputJob.setOutputValueClass(VectorWritable.class);

		FileInputFormat.addInputPath(centroidInputJob, dataInput);
		FileOutputFormat.setOutputPath(centroidInputJob, centroidsPath);
		centroidInputJob.setNumReduceTasks(0);

		if (!centroidInputJob.waitForCompletion(true)) {
			System.err.println("Centroid input job failed!");
			System.exit(1);
		}

		logger.info("数据处理阶段完成，开始聚类迭代......");

		/**
		 * 作业链3：循环迭代
		 */
		int iteration = 1;
		long changes = 0;
		do {
			Configuration iterConf = new Configuration();
			iterConf.setInt(KMeansConstant.CLUSTERS, nClusters);
			iterConf.setFloat(KMeansConstant.TOLERANCE, tolerance);

			Path prevIter = new Path(centroidsPath.getParent(), String.format("centroids_%s", iteration - 1));
			Path nextIter = new Path(centroidsPath.getParent(), String.format("centroids_%s", iteration));
			iterConf.set(KMeansConstant.CENTROIDS, prevIter.toString());
			Job iterJob = new Job(iterConf);
			iterJob.setJobName("KMeans-Cluster-Iteration-" + iteration);
			iterJob.setJarByClass(KMeansClusterDistribute.class);
			HDFSUtils.delete(iterConf, nextIter);

			// 输入输出数据格式
			iterJob.setInputFormatClass(SequenceFileInputFormat.class);
			iterJob.setOutputFormatClass(SequenceFileOutputFormat.class);

			// 设置Mapper, Combiner, Reducer
			iterJob.setMapperClass(KMeansMapper.class);
			iterJob.setCombinerClass(KMeansCombiner.class);
			iterJob.setReducerClass(KMeansReducer.class);

			// 设置MapReduce键值格式
			iterJob.setMapOutputKeyClass(IntWritable.class);
			iterJob.setMapOutputValueClass(VectorWritable.class);
			iterJob.setOutputKeyClass(IntWritable.class);
			iterJob.setOutputValueClass(VectorWritable.class);

			// 设置输出路径
			FileInputFormat.addInputPath(iterJob, data);
			FileOutputFormat.setOutputPath(iterJob, nextIter);

			iterJob.setNumReduceTasks(nReducers);

			if (!iterJob.waitForCompletion(true)) {
				System.err.println("ERROR: Iteration " + iteration + " failed!");
				System.exit(1);
			}
			iteration++;
			changes = iterJob.getCounters().findCounter(KMeansConstant.Counter.CONVERGED).getValue();
			iterJob.getCounters().findCounter(KMeansConstant.Counter.CONVERGED).setValue(0);
		} while (changes > 0);

		logger.info("Number of iterations: " + (iteration - 1));

		/**
		 * 读取聚类中心，并输出
		 */
		Path prevIter = new Path(centroidsPath.getParent(), String.format("centroids_%s", iteration - 1));
		Configuration finalConf = getConf();
		FileSystem fs = prevIter.getFileSystem(finalConf);
		Path pathPattern = new Path(prevIter, "part-*");
		FileStatus[] list = fs.globStatus(pathPattern);
		for (FileStatus status : list) {
			SequenceFile.Reader reader = null;
			try {
				reader = new SequenceFile.Reader(fs, status.getPath(), finalConf);
				IntWritable key = (IntWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				VectorWritable value = (VectorWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				while (reader.next(key, value)) {
					System.out.print(String.format("Centroid %s: ", key.get()));
					Vector<Double> vector = value.getVector();
					for (int i = 0; i < vector.size(); ++i) {
						System.out.print(String.format("%.2f", vector.get(i)));
						if (i < vector.size() - 1) {
							System.out.print(",");
						}
					}
					System.out.println();
				}
			} finally {
				reader.close();
			}
		}

		return 0;
	}

	/**
	 * 主函数
	 */
	public static void main(String[] args) {
		try {
			int exitCode = ToolRunner.run(new KMeansClusterDistribute(), args);
			System.exit(exitCode);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
