package zx.soft.kmeans.cluster.mapred.simple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * In Mapper class we are overriding configure function. In this we are
 * reading file from Distributed Cache and then storing that into instance
 * variable "mCenters"
 */
public class KMeansMapper extends MapReduceBase implements Mapper<LongWritable, Text, DoubleWritable, DoubleWritable> {

	@Override
	public void configure(JobConf job) {
		try {
			// Fetch the file from Distributed Cache Read it and store the
			// centroid in the ArrayList
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
			if (cacheFiles != null && cacheFiles.length > 0) {
				String line;
				KMeansMapReduce.mCenters.clear();
				BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				try {
					// Read the file split by the splitter and store it in
					// the list
					while ((line = cacheReader.readLine()) != null) {
						String[] temp = line.split(KMeansMapReduce.SPLITTER);
						KMeansMapReduce.mCenters.add(Double.parseDouble(temp[0]));
					}
				} finally {
					cacheReader.close();
				}
			}
		} catch (IOException e) {
			System.err.println("Exception reading DistribtuedCache: " + e);
		}
	}

	/*
	 * Map function will find the minimum center of the point and emit it to
	 * the reducer
	 */
	@Override
	public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, DoubleWritable> output,
			Reporter reporter) throws IOException {
		String line = value.toString();
		double point = Double.parseDouble(line);
		double min1, min2 = Double.MAX_VALUE, nearest_center = KMeansMapReduce.mCenters.get(0);
		// Find the minimum center from a point
		for (double c : KMeansMapReduce.mCenters) {
			min1 = c - point;
			if (Math.abs(min1) < Math.abs(min2)) {
				nearest_center = c;
				min2 = min1;
			}
		}
		// Emit the nearest center and the point
		output.collect(new DoubleWritable(nearest_center), new DoubleWritable(point));
	}

}
