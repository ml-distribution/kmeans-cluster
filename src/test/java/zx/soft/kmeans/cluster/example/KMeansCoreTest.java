package zx.soft.kmeans.cluster.example;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class KMeansCoreTest {

	@Test
	public void testInit() {
		NDimNode[] nodes = new NDimNode[3];
		nodes[0] = new NDimNode(2);
		nodes[1] = new NDimNode(2);
		nodes[2] = new NDimNode(2);
		KMeansCore kMeansCore = new KMeansCore();
		Params params = kMeansCore.init(nodes, 5);
		assertEquals(5, params.getK());
		assertEquals(2, params.getDim());
		assertEquals(0.0, params.getLogLikelihood(), 0.0);
		assertEquals(0.0, params.getMinDescLen(), 0.0);
		assertEquals(0, params.getNumOfIters());
		assertEquals(3, params.getNumOfNodes());
		assertEquals(5, params.getCentroids().length);
		assertEquals(5, params.getNodesInCluster().size());
		assertEquals(5, params.getPrior().length);
	}

}
