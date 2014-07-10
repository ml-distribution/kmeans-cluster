package zx.soft.kmeans.cluster.mapred.simple;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KMeansReducer extends MapReduceBase implements
		Reducer<DoubleWritable, DoubleWritable, DoubleWritable, Text> {

	/*
	 * Reduce function will emit all the points to that center and calculate
	 * the next center for these points
	 */
	@Override
	public void reduce(DoubleWritable key, Iterator<DoubleWritable> values,
			OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
		double newCenter;
		double sum = 0;
		int no_elements = 0;
		String points = "";
		while (values.hasNext()) {
			double d = values.next().get();
			points = points + " " + Double.toString(d);
			sum = sum + d;
			++no_elements;
		}

		// We have new center now
		newCenter = sum / no_elements;

		// Emit new center and point
		output.collect(new DoubleWritable(newCenter), new Text(points));
	}

}
