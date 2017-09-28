package org.apache.flink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.tools.*;
import org.apache.flink.util.Collector;

/**
 *
 */
public class MapPhase1 extends RichFlatMapFunction<String, CurveRecord>
		implements FlatMapFunction<String, CurveRecord> {

	private static final long serialVersionUID = 1L;
	private int fileId = 0;
	private ExecConf conf;

	/**
	 *
	 * @param fileId
	 * @param conf
	 */
	public MapPhase1(int fileId, ExecConf conf) {
		this.fileId = fileId;
		this.conf = conf;
	}

	/**
	 *
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	@Override
	public void flatMap(String input, Collector<CurveRecord> output)
			throws Exception {

		String val = null;
		String line = input;
		char ch = ',';
		int pos = line.indexOf(ch);
		String id = line.substring(0, pos);
		String rest = line.substring(pos + 1, line.length()).trim();
		String[] parts = rest.split(",");
		float[] coord = new float[conf.getDimension()];
		for (int i = 0; i < conf.getDimension(); i++) {
			float tmp = 0;
			try {
				tmp = Float.valueOf(parts[i]);
			} catch (NumberFormatException e) {
				continue;
			}
			if (tmp > -9999)
				coord[i] = Float.valueOf(parts[i]);
		}

		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		for (int i = 0; i < conf.getShift(); i++) {
			//float[] tmp_coord = new float[conf.getDimension()];
			int[] converted_coord = new int[conf.getDimension()];
			for (int k = 0; k < conf.getDimension(); k++) {
				//tmp_coord[k] = coord[k];
				//converted_coord[k] = (int) tmp_coord[k];
				//tmp_coord[k] -= converted_coord[k];
				coord[k] = coord[k] * conf.getScale()[k];
				converted_coord[k] = (int) (coord[k] * 10);
				//converted_coord[k] += (tmp_coord[k] * conf.getScale()[k]);
				if (i != 0) converted_coord[k] += conf.getShiftVectors()[i][k];
			}

			if (conf.getHOrZOrG() == 1) val = Horder.valueOf(converted_coord, conf);
			else if (conf.getHOrZOrG() == 2) val = Zorder.valueOf(conf.getDimension(), converted_coord);
			else if (conf.getHOrZOrG() == 3) val = Gorder.valueOf(conf.getDimension(), converted_coord);
			else System.out.println("Error! Wrong curve code!");

			if (conf.getClassifyOrRegress() == 1) {
				output.collect(new CurveRecord(val, id, fileId, i, Float.valueOf(parts[conf.getDimension() + 1])));
			} else if (conf.getClassifyOrRegress() == 2) {
				output.collect(new CurveRecord(val, id, fileId, i, Float.valueOf(parts[conf.getDimension()])));
			}
		}
	}
}