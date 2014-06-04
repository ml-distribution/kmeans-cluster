package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Shannon Quinn
 * 
 * Configures and runs the KMeans algorithm.
 */
public class KMeansDriver extends Configured implements Tool {

	static enum Counter {
		CONVERGED
	}

	public static final String ITERATIONS = "com.magsol.bigdata.hw7.converged";
	public static final String CLUSTERS = "com.magsol.bigdata.hw7.clusters";
	public static final String TOLERANCE = "com.magsol.bigdata.hw7.tolerance";
	public static final String CENTROIDS = "com.magsol.bigdata.hw7.centroids";

	/**
	 * Makes multiple runs on the same path easier.
	 * @param conf
	 * @param path
	 * @throws IOException
	 */
	public static void delete(Configuration conf, Path path) throws IOException {
		FileSystem fs = path.getFileSystem(conf);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

	/**
	 * Reads centroids from HDFS.
	 * @param conf
	 * @param path
	 * @throws IOException
	 */
	public static HashMap<Integer, VectorWritable> readCentroids(Configuration conf, Path path) throws IOException {
		HashMap<Integer, VectorWritable> centroids = new HashMap<Integer, VectorWritable>();
		FileSystem fs = FileSystem.get(path.toUri(), conf);
		FileStatus[] list = fs.globStatus(new Path(path, "part-*"));
		for (FileStatus status : list) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, status.getPath(), conf);
			IntWritable key = null;
			VectorWritable value = null;
			try {
				key = (IntWritable) reader.getKeyClass().newInstance();
				value = (VectorWritable) reader.getValueClass().newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			while (reader.next(key, value)) {
				centroids.put(new Integer(key.get()),
						new VectorWritable(value.get(), value.getClusterId(), value.getNumInstances()));
			}
			reader.close();
		}
		return centroids;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		// Read in the command line arguments.
		Path dataInput = new Path(conf.get("input"));
		Path centroids = new Path(conf.get("centroids"));
		Path output = new Path(conf.get("output"));
		int nClusters = conf.getInt("n_clusters", 8); // scikit-learn style
		int nReducers = conf.getInt("reducers", 10);
		float tolerance = conf.getFloat("tolerance", 1e-6F);

		// Job 0a: Read in the cluster centroids. This job could potentially
		// be removed if/when Canopy clusering is brought up.
		Configuration centroidConf = new Configuration();
		Job centroidInputJob = new Job(centroidConf);
		centroidInputJob.setJobName("KMeans Centroid Input");
		centroidInputJob.setJarByClass(KMeansDriver.class);
		Path centroidsPath = new Path(output.getParent(), "centroids_0");
		KMeansDriver.delete(centroidConf, centroidsPath);

		centroidInputJob.setInputFormatClass(TextInputFormat.class);
		centroidInputJob.setOutputFormatClass(SequenceFileOutputFormat.class);

		centroidInputJob.setMapperClass(KMeansCentroidInputMapper.class);
		// No Combiner, no Reducer.

		centroidInputJob.setMapOutputKeyClass(IntWritable.class);
		centroidInputJob.setMapOutputValueClass(VectorWritable.class);
		centroidInputJob.setOutputKeyClass(IntWritable.class);
		centroidInputJob.setOutputValueClass(VectorWritable.class);

		FileInputFormat.addInputPath(centroidInputJob, centroids);
		FileOutputFormat.setOutputPath(centroidInputJob, centroidsPath);
		centroidInputJob.setNumReduceTasks(0);

		if (!centroidInputJob.waitForCompletion(true)) {
			System.err.println("Centroid input job failed!");
			System.exit(1);
		}

		// Job 0b: Read in the data. For now, cluster centroids are randomly
		// assigned to try and make them more or less uniform (not that it
		// matters, just having fun). For later, this will be where Canopy
		// clustering happens.
		Configuration dataConf = new Configuration();
		dataConf.setInt(KMeansDriver.CLUSTERS, nClusters);
		Job inputDataJob = new Job(dataConf);
		inputDataJob.setJobName("KMeans Data Input / Canopy");
		inputDataJob.setJarByClass(KMeansDriver.class);
		Path data = new Path(output.getParent(), "formattedData");
		KMeansDriver.delete(dataConf, data);

		inputDataJob.setInputFormatClass(TextInputFormat.class);
		inputDataJob.setOutputFormatClass(SequenceFileOutputFormat.class);

		inputDataJob.setMapperClass(KMeansDataInputMapper.class);
		// No Combiner, no Reducer.

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

		// Preprocessing is done. Now on to the clustering.

		// Loop!
		int iteration = 1;
		long changes = 0;
		do {
			Configuration iterConf = new Configuration();
			iterConf.setInt(KMeansDriver.CLUSTERS, nClusters);
			iterConf.setFloat(KMeansDriver.TOLERANCE, tolerance);

			Path nextIter = new Path(centroidsPath.getParent(), String.format("centroids_%s", iteration));
			Path prevIter = new Path(centroidsPath.getParent(), String.format("centroids_%s", iteration - 1));
			iterConf.set(KMeansDriver.CENTROIDS, prevIter.toString());
			Job iterJob = new Job(iterConf);
			iterJob.setJobName("KMeans " + iteration);
			iterJob.setJarByClass(KMeansDriver.class);
			KMeansDriver.delete(iterConf, nextIter);

			// Set input/output formats.
			iterJob.setInputFormatClass(SequenceFileInputFormat.class);
			iterJob.setOutputFormatClass(SequenceFileOutputFormat.class);

			// Set Mapper, Reducer, Combiner
			iterJob.setMapperClass(KMeansMapper.class);
			iterJob.setCombinerClass(KMeansCombiner.class);
			iterJob.setReducerClass(KMeansReducer.class);

			// Set MR formats.
			iterJob.setMapOutputKeyClass(IntWritable.class);
			iterJob.setMapOutputValueClass(VectorWritable.class);
			iterJob.setOutputKeyClass(IntWritable.class);
			iterJob.setOutputValueClass(VectorWritable.class);

			// Set input/output paths.
			FileInputFormat.addInputPath(iterJob, data);
			FileOutputFormat.setOutputPath(iterJob, nextIter);

			iterJob.setNumReduceTasks(nReducers);

			if (!iterJob.waitForCompletion(true)) {
				System.err.println("ERROR: Iteration " + iteration + " failed!");
				System.exit(1);
			}
			iteration++;
			changes = iterJob.getCounters().findCounter(KMeansDriver.Counter.CONVERGED).getValue();
			iterJob.getCounters().findCounter(KMeansDriver.Counter.CONVERGED).setValue(0);
		} while (changes > 0);
		System.out.println("Number of iterations: " + (iteration - 1));

		// Finally, read through the centroids and print them out.
		Path prevIter = new Path(centroidsPath.getParent(), String.format("centroids_%s", iteration - 1));
		Configuration finalConf = getConf();
		FileSystem fs = prevIter.getFileSystem(finalConf);
		Path pathPattern = new Path(prevIter, "part-*");
		FileStatus[] list = fs.globStatus(pathPattern);
		for (FileStatus status : list) {
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, status.getPath(), finalConf);
			IntWritable key = null;
			VectorWritable value = null;
			try {
				key = (IntWritable) reader.getKeyClass().newInstance();
				value = (VectorWritable) reader.getValueClass().newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			while (reader.next(key, value)) {
				System.out.print(String.format("Centroid %s: ", key.get()));
				Vector<Double> v = value.get();
				for (int i = 0; i < v.size(); ++i) {
					System.out.print(String.format("%.2f", v.get(i).doubleValue()));
					if (i < v.size() - 1) {
						System.out.print(",");
					}
				}
				System.out.println();
			}
			reader.close();
		}

		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new KMeansDriver(), args));
	}

}
