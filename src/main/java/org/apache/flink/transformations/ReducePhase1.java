package org.apache.flink.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.tools.CurveRecord;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.tools.Functions;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;

/**
 *
 */
@FunctionAnnotation.ReadFields("first; third; fourth")
public class ReducePhase1 extends RichGroupReduceFunction<CurveRecord, String>
		implements GroupReduceFunction<CurveRecord, String> {

	private static final long serialVersionUID = 1L;
	private ExecConf conf;

	/**
	 *
	 * @param conf
	 */
	public ReducePhase1(ExecConf conf) {
		this.conf = conf;
	}

	/**
	 *
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	@Override
	public void reduce(Iterable<CurveRecord> input, Collector<String> output)
			throws Exception {

		LinkedList<String> RtmpList = new LinkedList<String>();
		LinkedList<String> StmpList = new LinkedList<String>();

		String shiftId = "";

		Iterator<CurveRecord> iterator = input.iterator();
		while (iterator.hasNext()) {
			CurveRecord entry = iterator.next();
			if (entry.getThird() == 0) {
				RtmpList.add(entry.getFirst());
				shiftId = entry.getFourth().toString();
			} else {
				StmpList.add(entry.getFirst());
				shiftId = entry.getFourth().toString();
			}
		}

		int Rsize = RtmpList.size();
		int Ssize = StmpList.size();

		ValueComparator com = new ValueComparator();
		Collections.sort(RtmpList, com);
		Collections.sort(StmpList, com);

		String q_start = "";
		int len = Functions.maxDecDigits(conf.getDimension());
		q_start = Functions.createExtra(len);

		// *********************** Estimate ranges ****************************//
		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		for (int i = 1; i <= conf.getNumOfPartition(); i++) {

			int estRank = Functions.getEstimatorIndex(i, conf.getNr(),
					conf.getSampleRateOfR(), conf.getNumOfPartition());
			if (estRank - 1 >= Rsize)
				estRank = Rsize;

			String q_end;
			if (i == conf.getNumOfPartition()) {
				q_end = Functions.maxDecString(conf.getDimension());
			} else
				q_end = RtmpList.get(estRank - 1);

			output.collect("(" + shiftId + "," + "0," + q_start + " " + q_end + ")");

			int low;
			if (i == 1)
				low = 0;
			else {
				int newKnn = (int) Math
						.ceil((double) conf.getKnn()
								/ (conf.getEpsilon() * conf.getEpsilon() * conf.getNs()));
				low = Collections.binarySearch(StmpList, q_start);
				if (low < 0)
					low = -low - 1;
				if ((low - newKnn) < 0)
					low = 0;
				else
					low -= newKnn;
			}

			String s_start;
			if (i == 1) {
				len = Functions.maxDecDigits(conf.getDimension());
				s_start = Functions.createExtra(len);
			} else
				s_start = StmpList.get(low);

			int high;
			if (i == conf.getNumOfPartition()) {
				high = Ssize - 1;
			} else {
				int newKnn = (int) Math
						.ceil((double) conf.getKnn()
								/ (conf.getEpsilon() * conf.getEpsilon() * conf
								.getNs()));
				high = Collections.binarySearch(StmpList, q_end);
				if (high < 0)
					high = -high - 1;
				if ((high + newKnn) > Ssize - 1)
					high = Ssize - 1;
				else
					high += newKnn;
			}

			String s_end = null;
			if (i == conf.getNumOfPartition()) {
				s_end = Functions.maxDecString(conf.getDimension());
			} else {
				s_end = StmpList.get(high);
			}

			output.collect("(" + shiftId + "," + "1," + s_start + " " + s_end + ")");

			q_start = q_end;
		}
	}

	/**
	 *
	 */
	private class ValueComparator implements Comparator<String> {

		/**
		 *
		 * @param w1
		 * @param w2
		 * @return
		 */
		@Override
		public int compare(String w1, String w2) {

			int cmp = w1.compareTo(w2);
			if (cmp != 0)
				return cmp;
			cmp = w1.toString().compareTo(w2.toString());

			return cmp;
		}
	}
}
