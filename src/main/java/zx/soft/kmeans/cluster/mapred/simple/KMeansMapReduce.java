package zx.soft.kmeans.cluster.mapred.simple;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class KMeansMapReduce {

	public static String OUT = "outfile";
	public static String IN = "inputlarger";
	public static String CENTROID_FILE_NAME = "/centroid.txt";
	public static String OUTPUT_FILE_NAME = "/part-00000";
	public static String DATA_FILE_NAME = "/data.txt";
	public static String JOB_NAME = "KMeansMapReduce";
	public static String SPLITTER = "\t| ";
	public static List<Double> mCenters = new ArrayList<Double>();

	public static void main(String[] args) throws Exception {
		run(args);
	}

	public static void run(String[] args) throws Exception {

		IN = args[0];
		OUT = args[1];
		String input = IN;
		String output = OUT + System.nanoTime();
		String again_input = output;

		// Reiterating till the convergence
		int iteration = 0;
		boolean isdone = false;
		while (isdone == false) {
			JobConf conf = new JobConf(KMeansMapReduce.class);
			if (iteration == 0) {
				Path hdfsPath = new Path(input + CENTROID_FILE_NAME);
				// upload the file to hdfs. Overwrite any existing copy.
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			} else {
				Path hdfsPath = new Path(again_input + OUTPUT_FILE_NAME);
				// upload the file to hdfs. Overwrite any existing copy.
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			}

			conf.setJobName(JOB_NAME);
			conf.setMapOutputKeyClass(DoubleWritable.class);
			conf.setMapOutputValueClass(DoubleWritable.class);
			conf.setOutputKeyClass(DoubleWritable.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(KMeansMapper.class);
			conf.setReducerClass(KMeansReducer.class);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(input + DATA_FILE_NAME));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			JobClient.runJob(conf);

			Path ofile = new Path(output + OUTPUT_FILE_NAME);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(ofile)));
			List<Double> centers_next = new ArrayList<Double>();
			String line = br.readLine();
			while (line != null) {
				String[] sp = line.split("\t| ");
				double c = Double.parseDouble(sp[0]);
				centers_next.add(c);
				line = br.readLine();
			}
			br.close();

			String prev;
			if (iteration == 0) {
				prev = input + CENTROID_FILE_NAME;
			} else {
				prev = again_input + OUTPUT_FILE_NAME;
			}
			Path prevfile = new Path(prev);
			FileSystem fs1 = FileSystem.get(new Configuration());
			BufferedReader br1 = new BufferedReader(new InputStreamReader(fs1.open(prevfile)));
			List<Double> centers_prev = new ArrayList<Double>();
			String l = br1.readLine();
			while (l != null) {
				String[] sp1 = l.split(SPLITTER);
				double d = Double.parseDouble(sp1[0]);
				centers_prev.add(d);
				l = br1.readLine();
			}
			br1.close();

			// Sort the old centroid and new centroid and check for convergence
			// condition
			Collections.sort(centers_next);
			Collections.sort(centers_prev);

			Iterator<Double> it = centers_prev.iterator();
			for (double d : centers_next) {
				double temp = it.next();
				if (Math.abs(temp - d) <= 0.1) {
					isdone = true;
				} else {
					isdone = false;
					break;
				}
			}
			++iteration;
			again_input = output;
			output = OUT + System.nanoTime();
		}
	}

}
