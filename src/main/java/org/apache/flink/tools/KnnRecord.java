package org.apache.flink.tools;

import java.io.Serializable;

/**
 *
 */
public class KnnRecord implements Serializable {

	private static final long serialVersionUID = 1L;
	private String rid;
	private float dist;
	private int[] coord;

	/**
	 *
	 * @param rid
	 * @param dist
	 * @param coord
	 */
	public KnnRecord(String rid, float dist, int[] coord) {
		this.rid = rid;
		this.dist = dist;
		this.coord = coord;
	}

	/**
	 *
	 * @return
	 */
	public float getDist() {
		return this.dist;
	}

	/**
	 *
	 * @return
	 */
	public String getRid() {
		return this.rid;
	}

	/**
	 *
	 * @return
	 */
	public String toString() {
		return this.rid + " " + Float.toString(this.dist);
	}

	/**
	 *
	 * @return
	 */
	public int[] getCoord() {
		return coord;
	}

	/**
	 *
	 * @param coord
	 */
	public void setCoord(int[] coord) {
		this.coord = coord;
	}
}