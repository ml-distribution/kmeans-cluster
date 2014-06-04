package zx.soft.kmeans.cluster.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Simply reads each line of the feature vectors and outputs them as 
 * VectorWritables. Initially assigns them a random clusterId (ideally making
 * somewhat even clusters), but this could be extended to perform Canopy
 * clustering (in which case, the centroids are in the DistributedCache).
 * 
 * @author wgybzb
 *
 */
public class KMeansDataInputMapper extends Mapper<LongWritable, Text, IntWritable, VectorWritable> {

	// private int nClusters;
	ArrayList<VectorWritable> centroids;

	@Override
	protected void setup(Context context) throws IOException {
		// If we're assigning random clusters:
		//nClusters = context.getConfiguration().getInt(KMeansDriver.CLUSTERS, 8);

		// If we're doing Canopy clustering:
		centroids = new ArrayList<VectorWritable>();
		/*
		// Need the list of VectorWritables.
		Path [] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (files == null || files.length < 1) {
		    throw new IOException("DistributedCache returned an empty file set!");
		}

		// Read in the shards from the DistributedCache.
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
		String[] elements = value.toString().trim().split(",");
		if (!elements[0].equals("caseid")) {
			// We have a line of data.
			Vector<Double> out = new Vector<Double>(elements.length - 1);
			for (int i = 1; i < elements.length; ++i) {
				out.add(Double.parseDouble(elements[i]));
			}

			// Write the points out to no cluster in particular.
			context.write(new IntWritable(-1), new VectorWritable(out, -1));
		}
	}

}
