package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shannon Quinn
 * 
 * Reads in a combination of instances and partial cluster centroids
 * (differentiated by their "numInstances" field), depending on how much Hadoop
 * decided to utilize Combiners. Uses this information to compute final
 * centroids for each cluster. 
 */
public class KMeansReducer extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	private HashMap<Integer, VectorWritable> centroids;

	@Override
	protected void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		Path centroidsPath = new Path(conf.get(KMeansDriver.CENTROIDS));
		centroids = KMeansDriver.readCentroids(conf, centroidsPath);
	}

	@Override
	public void reduce(IntWritable clusterId, Iterable<VectorWritable> values, Context context) throws IOException,
			InterruptedException {
		Vector<Double> centroid = new Vector<Double>();
		int num = 0;
		for (VectorWritable v : values) {
			Vector<Double> instance = v.get();
			if (instance.size() > 0) {
				centroid = KMeansCombiner.add(centroid, instance);
				num += v.getNumInstances();
			}
		}

		// Is this an empty centroid?
		if (num == 0) {
			// Write out the previous centroid for this cluster.
			context.write(clusterId, centroids.get(new Integer(clusterId.get())));
		} else {
			// Average the values. Also record the residual sum of squares
			// difference between the current value and the previous one.
			double residual = 0.0;
			Vector<Double> previous = centroids.get(new Integer(clusterId.get())).get();
			for (int i = 0; i < centroid.size(); ++i) {
				double value = centroid.get(i).doubleValue() / num;
				residual += (value - previous.get(i).doubleValue());
				centroid.set(i, new Double(value));
			}

			// Did this centroid change between iterations?
			float tolerance = context.getConfiguration().getFloat(KMeansDriver.TOLERANCE, 0.000001F);
			if (residual > tolerance) {
				context.getCounter(KMeansDriver.Counter.CONVERGED).increment(1);
			}

			// Write out the new centroid.
			context.write(clusterId, new VectorWritable(centroid, clusterId.get(), num));
		}
	}

}