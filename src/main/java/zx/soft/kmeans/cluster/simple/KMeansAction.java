package zx.soft.kmeans.cluster.simple;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.util.ArrayList;

/**
 * This KMeansAction performs a K-means clustering action on a BufferedImage
 * @author Patrick van Kouteren 
 *
 */

public class KMeansAction {

	BufferedImage image_temp;
	boolean not_terminated;
	int loops, changedPixels;
	int[] histogram;
	ArrayList<ClusterClass> classes;
	int[] lowerbounds;
	public final static int MEAN_BY_MOD = 1;
	public final static int MEAN_BY_SPACE = 2;
	public final static int MEAN_AT_RANDOM = 3;

	/**
	 * Controls the actual work:
	 * - Initialization
	 * - Loop until termination condition is met
	 *  + for each pixel: assign pixel to a class such that the distance from the pixel to the mean of that class is minimized
	 *  + for each class: recalculate the means of the class based on pixels belonging to that class
	 * - End loop
	 * @param image
	 * @param bins (k)
	 * @param histogram
	 */
	public KMeansAction(BufferedImage image, int bins, int[] histogram, int initway) {
		this.histogram = histogram;
		lowerbounds = new int[bins];
		initialize(image, bins, initway);
		calculateBounds();
		while (not_terminated) {
			recalculateMeans();
			loops++;
			checkTermination();
		}
		processImage(image, bins);
	}

	/**
	 * Set the new color values for the image
	 * @param image
	 */
	private void processImage(BufferedImage image, int bins) {
		int delta = 255 / (bins - 1);
		for (int h = 0; h < image.getHeight(); h++) {
			for (int w = 0; w < image.getWidth(); w++) {
				Color rgb = new Color(image.getRGB(w, h));
				int grey = rgb.getRed();
				for (int i = 0; i < classes.size(); i++) {
					if (grey > classes.get(i).lowerbound && grey < classes.get(i).upperbound) {
						int g = i * delta;
						image_temp.setRGB(w, h, (new Color(g, g, g)).getRGB());
					}
				}
			}
		}
	}

	/**
	 * Returns the image created by the processImage method
	 * @return the result image
	 */
	public BufferedImage getResultImage() {
		return image_temp;
	}

	/**
	 * Just for fun: returns the number of loops which were needed for getting a stable result
	 * @return number of loops for stable result
	 */
	public int getLoops() {
		return loops;
	}

	/**
	 * Initializes the algorithm. Creates k ClusterClasses and puts them into a LinkedList
	 * @param image
	 * @param bins
	 */
	@SuppressWarnings("unchecked")
	private void initialize(BufferedImage image, int bins, int initway) {
		image_temp = image;
		loops = 0;
		changedPixels = 0;
		not_terminated = true;
		classes = new ArrayList<ClusterClass>();
		for (int i = 0; i < bins; i++) {
			ClusterClass cc = new ClusterClass(createMean(initway, bins, i, image));
			classes.add(cc);
		}

	}

	/**
	 * Controls the calculations of the upper- and lowerbounds of ClusterClasses and sets them
	 *
	 */
	private void calculateBounds() {
		for (int i = 0; i < classes.size(); i++) {
			int lb = calculateLowerBound(classes.get(i));
			lowerbounds[i] = lb;
			classes.get(i).setBounds(lb, calculateUpperBound(classes.get(i)));
		}
	}

	/**
	 * Does the actual calculation of the lowerbound
	 * @param ClusterClass
	 * @return Lowerbound
	 */
	private int calculateLowerBound(ClusterClass cc) {
		int cMean = cc.getMean();
		int currentBound = 0;
		for (int i = 0; i < classes.size(); i++) {
			if (cMean > classes.get(i).getMean()) {
				currentBound = Math.max((cMean + classes.get(i).getMean()) / 2, currentBound);
			} else {
			}
		}
		return currentBound;
	}

	/**
	 * Does the actual calculation of the upperbound
	 * @param ClusterClass
	 * @return Upperbound
	 */
	private int calculateUpperBound(ClusterClass cc) {
		int cMean = cc.getMean();
		int currentBound = 255;
		for (int i = 0; i < classes.size(); i++) {
			if (cMean < classes.get(i).getMean()) {
				currentBound = Math.min((cMean + classes.get(i).getMean()) / 2, currentBound);
			} else {
			}
		}
		return currentBound;
	}

	/**
	 * Takes care of the recalculation of the means of the ClusterClasses
	 *
	 */
	private void recalculateMeans() {
		for (int i = 0; i < classes.size(); i++) {
			classes.get(i).calculateMean(histogram);
		}
		calculateChangedPixels();
	}

	/**
	 * Checks if the termination condition is met
	 *
	 */
	private void checkTermination() {
		if (loops >= 50) {
			not_terminated = false;
		}
		if (changedPixels <= 300) {
			not_terminated = false;
		}
	}

	private void calculateChangedPixels() {
		int changed = 0;
		for (int i = 0; i < classes.size(); i++) {
			int c = calculateLowerBound(classes.get(i));
			if (c < lowerbounds[i]) {
				for (int j = c; j < lowerbounds[i]; j++) {
					changed += histogram[j];
				}
			}
			if (c > lowerbounds[i]) {
				for (int j = lowerbounds[i]; j < c; j++) {
					changed += histogram[j];
				}
			}
		}
		changedPixels = changed;
		calculateBounds();
	}

	/**
	 * This method returns a mean for a class, depending on the way the user wants the program to create a mean
	 * @return mean
	 */
	private int createMean(int initway, int bins, int index, BufferedImage image) {
		switch (initway) {
		case MEAN_BY_MOD:
			int pixelindex = 0;
			int sum = 0;
			int value = 0;
			for (int h = 0; h < image.getHeight(); h++) {
				for (int w = 0; w < image.getWidth(); w++) {
					pixelindex += 1;
					if (pixelindex % bins == index) {
						Color rgb = new Color(image.getRGB(w, h));
						sum += rgb.getRed();
						value += 1;
					}
				}
			}
			return sum / value;

		case MEAN_BY_SPACE:
			return 255 / (bins - 1) * index;
		case MEAN_AT_RANDOM:
			Double dmean = Math.random() * 255;
			return (int) Math.floor(dmean);
		default:
			return 0;
		}
	}
}
