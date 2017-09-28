package org.apache.flink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.tools.CurveRecord;
import org.apache.flink.tools.ExecConf;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 *
 */
public class MapPhase1Sampling implements FlatMapFunction<CurveRecord, CurveRecord> {

	private static final long serialVersionUID = 1L;
	private ExecConf conf;
	private Random r;
	private double sampleRate;

	/**
	 *
	 * @param conf
	 */
	public MapPhase1Sampling(ExecConf conf) {
		this.conf = conf;
		r = new Random();
	}

	/**
	 *
	 * @param value
	 * @param output
	 * @throws Exception
	 */
	@Override
	public void flatMap(CurveRecord value, Collector<CurveRecord> output) throws Exception {

		if (value.getThird() == 0)
			sampleRate = conf.getSampleRateOfR();
		else if (value.getThird() == 1)
			sampleRate = conf.getSampleRateOfS();
		else {
			System.out.println("Wrong source file!");
			System.exit(-1);
		}

		boolean sampled = false;
		if (r.nextDouble() < sampleRate)
			sampled = true;
		if (sampled) {
			output.collect(value);
		}
	}
}
