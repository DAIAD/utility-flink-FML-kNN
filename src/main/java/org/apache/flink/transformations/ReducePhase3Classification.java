package org.apache.flink.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 *
 */
public class ReducePhase3Classification implements
		GroupReduceFunction<Tuple4<String, String, String, String>, String> {

	private static final long serialVersionUID = 1L;
	private ExecConf conf;

	/**
	 *
	 * @param conf
	 */
	public ReducePhase3Classification(ExecConf conf) {
		this.conf = conf;
	}

	/**
	 *
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	@Override
	public void reduce(Iterable<Tuple4<String, String, String, String>> input,
					   Collector<String> output) throws Exception {

		Iterator<Tuple4<String, String, String, String>> iterator = input.iterator();
		String id1 = null;

		RecordComparator rc = new RecordComparator();
		PriorityQueue<Record> pq = new PriorityQueue<Record>(conf.getKnn() + 1, rc);
		TreeSet<String> ts = new TreeSet<String>();

		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		while (iterator.hasNext()) {
			Tuple4<String, String, String, String> entry = iterator.next();

			id1 = entry.f0;
			String id2 = entry.f1;

			if (!ts.contains(id2)) {
				ts.add(id2);
				float dist = Float.parseFloat(entry.f2);
				float theClass = 0;
				try {
					theClass = Float.parseFloat(entry.f3);
				} catch (Exception e) {
					System.out.println("");
				}
				Record record = new Record(id2, dist, theClass);
				pq.add(record);
				if (pq.size() > conf.getKnn())
					pq.poll();
			}

		}

		// Calculate kNN weights, class probabilities and classify
		Record[] kNNs = new Record[conf.getKnn()];
		int count = 0;
		while (pq.size() > 0) {
			kNNs[count] = pq.poll();
			count++;
		}

		float maxDistance = kNNs[0].dist;
		float minDistance = 0;
		try {
			minDistance = kNNs[conf.getKnn() - 1].dist;
		} catch (NullPointerException e) {
			System.out.println("Error element: " + id1);
			if (kNNs[conf.getKnn() - 1] != null)
				System.out.println("Error kNN: " + kNNs[conf.getKnn() - 1].id2);
			output.collect(new String(id1.toString() + " ERROR!"));
			return;
		}

		float[] weights = new float[conf.getKnn()];
		for (int i = 0; i < conf.getKnn(); i++) {
			if (maxDistance == minDistance)
				weights[i] = 1;
			else {
				weights[i] = (((maxDistance - kNNs[i].dist) / (maxDistance - minDistance)));
			}
		}

		float totalWeight = 0;
		HashMap<Float, Float> classVotes = new HashMap<>();
		for (int i = 0; i < conf.getKnn(); i++) {
			float currClass = kNNs[i].theClass;
			if (!classVotes.containsKey(currClass)) {
				classVotes.put(currClass, weights[i]);
				totalWeight += weights[i];
			} else {
				float weight = classVotes.get(currClass);
				weight += weights[i];
				classVotes.put(currClass, weight);
				totalWeight += weights[i];
			}
		}

		Object[] classes = classVotes.keySet().toArray();
		float resultClass = (float) classes[0];
		HashMap<Float, Float> classProb = new HashMap<>();
		for (int i = 0; i < classes.length; i++) {
			if (classVotes.get(classes[i]) > classVotes.get(resultClass)) {
				resultClass = (float) classes[i];
			}
			classProb.put((float) classes[i], classVotes.get(classes[i]) / totalWeight);
		}

		String results = " | Result " + resultClass + " | ";
		for (int i = 0; i < conf.getNoOfClasses(); i++) {
			if (classProb.containsKey((float) (i + 1)))
				results += "Class " + (i + 1) + " probability " + classProb.get((float) i + 1) + " | ";
			else
				results += "Class " + (i + 1) + " probability 0.0 | ";
		}

		output.collect(new String(id1.toString() + results));
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 */
	class Record {
		public String id2;
		public float dist;
		public float theClass;

		/**
		 *
		 * @param id2
		 * @param dist
		 * @param theClass
		 */
		Record(String id2, float dist, float theClass) {
			this.id2 = id2;
			this.dist = dist;
			this.theClass = theClass;
		}

		/**
		 *
		 * @return
		 */
		public String toString() {
			return id2 + " " + Float.toString(dist) + " "
					+ Float.toString(theClass);
		}
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 */
	class RecordComparator implements Comparator<Record> {

		/**
		 *
		 * @param o1
		 * @param o2
		 * @return
		 */
		public int compare(Record o1, Record o2) {
			int ret = 0;
			float dist = o1.dist - o2.dist;

			if (dist > 0)
				ret = 1;
			else
				ret = -1;
			return -ret;
		}
	}
}