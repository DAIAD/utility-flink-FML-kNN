package org.apache.flink.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeSet;

/**
 *
 */
public class ReducePhase3Regression implements
		GroupReduceFunction<Tuple4<String, String, String, String>, String> {

	private static final long serialVersionUID = 1L;
	private ExecConf conf;

	/**
	 *
	 * @param conf
	 */
	public ReducePhase3Regression(ExecConf conf) {
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
				float consumption = 0;
				try {
					consumption = Float.parseFloat(entry.f3);
				} catch (Exception e) {
					System.out.println("");
				}
				Record record = new Record(id2, dist, consumption);
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

		float result = 0;
		int cnt = 0;
		for (int i = 0; i < conf.getKnn(); i++) {
			try {
				result += kNNs[i].theValue;
				cnt++;
			} catch (Exception e) {
				continue;
			}
		}
		result = result / cnt;
		String final_result = " | Result: " + result;

		output.collect(new String(id1.toString() + final_result));
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 **/
	class Record {
		public String id2;
		public float dist;
		public float theValue;

		/**
		 *
		 * @param id2
		 * @param dist
		 * @param consumption
		 */
		Record(String id2, float dist, float consumption) {
			this.id2 = id2;
			this.dist = dist;
			this.theValue = consumption;
		}

		/**
		 *
		 * @return
		 */
		public String toString() {
			return id2 + " " + Float.toString(dist) + " " + Float.toString(theValue);
		}
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 **/
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