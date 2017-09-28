package org.apache.flink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tools.CurveRecord;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 *
 */
@FunctionAnnotation.ReadFields("first; third; fourth")
public class MapPhase2 extends RichFlatMapFunction<CurveRecord, CurveRecord>
		implements FlatMapFunction<CurveRecord, CurveRecord> {

	private static final long serialVersionUID = 1L;
	private String[][] RrangeArray;
	private String[][] SrangeArray;
	private ExecConf conf;
	private Collection<String> ranges;

	/**
	 *
	 * @param conf
	 * @throws IOException
	 */
	public MapPhase2(ExecConf conf) throws IOException {
		this.conf = conf;
	}

	/**
	 * Reads the values from a broadcast variable into a collection.
	 * @param parameters
	 * @throws Exception
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		this.ranges = getRuntimeContext().getBroadcastVariable("ranges");
		RrangeArray = new String[conf.getShift()][conf.getNumOfPartition()];
		SrangeArray = new String[conf.getShift()][conf.getNumOfPartition()];
		Object[] arrayRanges = this.ranges.toArray();
		int cnt1 = 0, cnt2 = 0, cnt3 = 0, cnt4 = 0;

		for (int i = 0; i < arrayRanges.length; i++) {
			String val = arrayRanges[i].toString();
			if (val.charAt(1) == '0') {
				if (val.charAt(3) == '0') {
					RrangeArray[0][cnt1++] = val.substring(2);
				} else {
					SrangeArray[0][cnt2++] = val.substring(2);
				}
			} else if (val.charAt(1) == '1') {
				if (val.charAt(3) == '0') {
					RrangeArray[1][cnt3++] = val.substring(2);
				} else {
					SrangeArray[1][cnt4++] = val.substring(2);
				}
			}
		}
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	@Override
	public void flatMap(CurveRecord input, Collector<CurveRecord> output)
			throws Exception {

		/* String line = input.toString().trim();
        String[] parts = line.split(" ");
        int zOffset, ridOffset, srcOffset, sidOffset, classOffset;
        zOffset = 0;
        ridOffset = zOffset + 1;
        srcOffset = ridOffset + 1;
        sidOffset = srcOffset + 1;
        classOffset = sidOffset + 1; */

		ArrayList<String> pidList = getPartitionId(input.getFirst(), input.getThird().toString(), input.getFourth()
				.toString());
		if (pidList.size() == 0) {
			System.out.println("Cannot get pid");
			System.exit(-1);
		}

		for (int i = 0; i < pidList.size(); i++) {
			String pid = pidList.get(i);
			int intSid = Integer.valueOf(input.getFourth());
			int intPid = Integer.valueOf(pid);
			int groupKey = intSid * conf.getNumOfPartition() + intPid;

			CurveRecord bp2v = new CurveRecord(input.getFirst(), input.getSecond(), input.getThird(), groupKey, input
					.getClassOrValue());
			output.collect(bp2v);
		}
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 * @param z
	 * @param src
	 * @param sid
	 * @return
	 * @throws IOException
	 */
	public ArrayList<String> getPartitionId(String z, String src, String sid)
			throws IOException {

		String ret = null;
		String[] mark = null;
		ArrayList<String> idList = new ArrayList<String>(
				conf.getNumOfPartition());

		if (src.compareTo("0") == 0)
			mark = RrangeArray[Integer.parseInt(sid)];
		else if (src.compareTo("1") == 0)
			mark = SrangeArray[Integer.parseInt(sid)];
		else {
			System.out.println(src);
			System.out.println("Unknown source for input record !!!");
			System.exit(-1);
		}

		for (int i = 0; i < conf.getNumOfPartition(); i++) {
			String range = mark[i];
			String[] parts = range.split(" +");
			String low = parts[0].substring(3);
			String high = parts[1].substring(0, parts[1].length() - 1);

			if (z.compareTo(low) >= 0 && z.compareTo(high) <= 0) {
				ret = Integer.toString(i);
				idList.add(ret);
			}
		}
		return idList;
	}
}