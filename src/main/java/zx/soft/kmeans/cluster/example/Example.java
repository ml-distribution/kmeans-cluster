package zx.soft.kmeans.cluster.example;

import java.io.Serializable;

/**
 * This Example class is a data structure that contains the class label and its attribute.
 * 
 * @author wanggang
 *
 */
public class Example implements Serializable {

	private static final long serialVersionUID = 5956728992101907728L;

	/********** Variables *************/
	private final int classLabel;
	private final double[] attributes;

	/********** Methods ***************/

	/** Constructor
	  * @param classLabel contains the class of this example
	  * @param attributes contains the array of attributes to be stored
	 **/
	public Example(int classLabel, double[] attributes) {
		this.classLabel = classLabel;
		this.attributes = new double[attributes.length];
		for (int i = 0; i < attributes.length; i++) {
			this.attributes[i] = attributes[i];
		}
	}

	/** Accessor for the classLabel
	  * @return classLabel
	 **/
	public int getClassLabel() {
		return classLabel;
	}

	/** Accessor for a single attribue in the attribute array
	  * @return attribute value in position index
	 **/
	public double getAttribute(int index) {
		return attributes[index];
	}

}