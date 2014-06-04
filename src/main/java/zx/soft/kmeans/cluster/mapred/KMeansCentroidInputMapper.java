package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Simply reads in the text versions of the centroids and writes them back
 * out as VectorWritables.
 * 
 * @author wgybzb
 *
 */
public class KMeansCentroidInputMapper extends Mapper<LongWritable, Text, IntWritable, VectorWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		// Parse the line of text to extract the feature vector and clusterId.
		String[] elements = value.toString().trim().split(",");
		Vector<Double> out = new Vector<Double>();

		// Write it out.
		int clusterId = Integer.parseInt(elements[0]);
		for (int i = 2; i < elements.length; ++i) {
			out.add(Double.parseDouble(elements[i]));
		}
		context.write(new IntWritable(clusterId), new VectorWritable(out, clusterId, 0));
	}

}
