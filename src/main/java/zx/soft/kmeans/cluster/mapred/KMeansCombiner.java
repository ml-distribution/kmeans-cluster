package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Shannon Quinn
 *
 * Creates an intermediate cluster centroid out of the limited number of local
 * data instances. This is meant to reduce the network traffic and ease the
 * load in the Reducer.
 */
public class KMeansCombiner extends Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

	@Override
	public void reduce(IntWritable clusterId, Iterable<VectorWritable> instances, Context context)
			throws InterruptedException, IOException {
		Vector<Double> partial = new Vector<Double>();
		int num = 0;

		// Loop through the vectors, adding them together into a partial centroid.
		for (VectorWritable v : instances) {
			Vector<Double> instance = v.get();
			if (instance.size() > 0) {
				partial = KMeansCombiner.add(partial, instance);
				num += 1;
			}
		}

		// Did we get any actual vectors? Or just dummies?
		if (num > 0) {
			context.write(clusterId, new VectorWritable(partial, clusterId.get(), num));
		} else {
			// Write out the dummy.
			context.write(clusterId, new VectorWritable(new Vector<Double>(), -1, -1));
		}
	}

	public static Vector<Double> add(Vector<Double> partial, Vector<Double> v) {
		Vector<Double> retval = new Vector<Double>(v.size());
		for (int i = 0; i < v.size(); ++i) {
			if (partial.size() < v.size()) {
				retval.insertElementAt(v.get(i), i);
			} else {
				retval.insertElementAt(new Double(partial.get(i).doubleValue() + v.get(i).doubleValue()), i);
			}
		}
		return retval;
	}

}
