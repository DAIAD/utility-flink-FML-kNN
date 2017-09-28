package org.apache.flink.tools;

import java.io.Serializable;

/**
 * Class that implements a record in one of the supported Space Filling Curves (z-order, Grey-code, Hilbert)
 * first, second, third and fourth values are generic
 */
public class CurveRecord implements Serializable {

	private static final long serialVersionUID = 1L;
	private String first;
	private String second;
	private Integer third;
	private Integer fourth;
	private Float classOrValue;

	/**
	 * Constructor 1
	 */
	public CurveRecord() {
	}

	/**
	 * Constructor 2
	 *
	 * @param first        first value
	 * @param second       second value
	 * @param third        third value
	 * @param fourth       fourth value
	 * @param classOrValue the class, or the value of the target variable, in case of classification or regression
	 */
	public CurveRecord(String first, String second, Integer third, Integer fourth, Float classOrValue) {
		this.first = first;
		this.second = second;
		this.third = third;
		this.fourth = fourth;
		this.classOrValue = classOrValue;
	}

	/**
	 * Get the first value
	 *
	 * @return first value
	 */
	public String getFirst() {
		return first;
	}

	/**
	 * Set the first value
	 *
	 * @param first the first value
	 */
	public void setFirst(String first) {
		this.first = first;
	}

	/**
	 * Get the second value
	 *
	 * @return second value
	 */
	public String getSecond() {
		return second;
	}

	/**
	 * Set the second value
	 *
	 * @param second the second value
	 */
	public void setSecond(String second) {
		this.second = second;
	}

	/**
	 * Get the third value
	 *
	 * @return third
	 */
	public Integer getThird() {
		return third;
	}

	/**
	 * Set the third value
	 *
	 * @param third the third value
	 */
	public void setThird(Integer third) {
		this.third = third;
	}

	/**
	 * Get the fourth value
	 *
	 * @return fourth value
	 */
	public Integer getFourth() {
		return fourth;
	}

	/**
	 * Set the fourth value
	 *
	 * @param fourth the fourth value
	 */
	public void setFourth(Integer fourth) {
		this.fourth = fourth;
	}

	/**
	 * Get the target class or value
	 *
	 * @return the target class or value
	 */
	public Float getClassOrValue() {
		return classOrValue;
	}

	/**
	 * Set the target class or value
	 *
	 * @param classOrValue the target class or value
	 */
	public void setClassOrValue(Float classOrValue) {
		this.classOrValue = classOrValue;
	}

	/**
	 * Convert all values to a concatenated string
	 *
	 * @return
	 */
	public String toString() {
		return first + " " + second.toString() + " " + third.toString() + " "
				+ fourth.toString() + " " + classOrValue.toString();
	}
}
