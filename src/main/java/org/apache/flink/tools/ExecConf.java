package org.apache.flink.tools;

import java.io.Serializable;

/**
 * Class that holds the configuration of the application. It is generated once and passed to every method that is
 * needed as an argument.
 */
public class ExecConf implements Serializable {

	private static final long serialVersionUID = 1L;

	private int knn = 15;
	private int shift = 2;
	private int nr = 27296;
	private int ns = 244169;
	private int dimension = 10;
	private int noOfClasses = 2;
	private double epsilon = 0.003;
	private int numOfPartition = 8;
	private int hOrZOrG = 2;
	private int classifyOrRegress = 2;
	private boolean crossValidation = false;
	private int crossValidationFolds = 10;
	private boolean geneticScaleCalc = false;
	private int[] scale = { 10, 9, 5, 5, 8, 3, 17, 8, 29, 5 };
	//private int[] scale = { 15, 10, 6, 10, 2, 9, 20, 3, 40 };
	//private int[] scale = {10, 9, 5, 5, 8, 3};
	private int[] scaleDomain = {10, 10, 10, 10, 10, 50, 50, 50, 10};
	private int[][] shiftVectors;
	private double sampleRateOfR;
	private double sampleRateOfS;
	private String hdfsPath = "";
	private String sourcesPath = "";

	/**
	 * Constructor
	 */
	public ExecConf() {
	}

	/**
	 * Returns the sampling rate of R
	 * @return sampling rate of R
	 */
	public double getSampleRateOfR() {
		return sampleRateOfR;
	}

	/**
	 * Sets the sampling rate of R
	 * @param sampleRateOfR the sampling rate of R
	 */
	public void setSampleRateOfR(double sampleRateOfR) {
		this.sampleRateOfR = sampleRateOfR;
	}

	/**
	 * Returns the sampling rate of S
	 * @return the sampling rate of S
	 */
	public double getSampleRateOfS() {
		return sampleRateOfS;
	}

	/**
	 * Sets the sampling rate of S
	 * @param sampleRateOfS the sampling rate of S
	 */
	public void setSampleRateOfS(double sampleRateOfS) {
		this.sampleRateOfS = sampleRateOfS;
	}

	/**
	 * Returns the k parameter
	 * @return the k parameter
	 */
	public int getKnn() {
		return knn;
	}

	/**
	 * Sets the k parameter
	 * @param knn the k parameter
	 */
	public void setKnn(int knn) {
		this.knn = knn;
	}

	/**
	 * Returns the number of shifts
	 * @return the number of shifts
	 */
	public int getShift() {
		return shift;
	}

	/**
	 * Sets the number of shifts
	 * @param shift the number of shifts
	 */
	public void setShift(int shift) {
		this.shift = shift;
	}

	/**
	 * Returns the size of the R dataset
	 * @return the size of the R dataset
	 */
	public int getNr() {
		return nr;
	}

	/**
	 * Sets the size of the R dataset
	 * @param nr the size of the R dataset
	 */
	public void setNr(int nr) {
		this.nr = nr;
	}

	/**
	 * Returns the size of the S dataset
	 * @return the size of the S dataset
	 */
	public int getNs() {
		return ns;
	}

	/**
	 * Sets the size of the S dataset
	 * @param ns the size of the S dataset
	 */
	public void setNs(int ns) {
		this.ns = ns;
	}

	/**
	 * Returns the number of dimensions
	 * @return the number of dimensions
	 */
	public int getDimension() {
		return dimension;
	}

	/**
	 * Sets the number of dimensions
	 * @param dimension the number of dimensions
	 */
	public void setDimension(int dimension) {
		this.dimension = dimension;
	}

	/**
	 * Returns the number of classes for classification
	 * @return the number of classes
	 */
	public int getNoOfClasses() {
		return noOfClasses;
	}

	/**
	 * Sets the number of classes for classification
	 * @param noOfClasses the number of classes
	 */
	public void setNoOfClasses(int noOfClasses) {
		this.noOfClasses = noOfClasses;
	}

	/**
	 * Returns the epsilon parameter for sampling
	 * @return the epsilon parameter
	 */
	public double getEpsilon() {
		return epsilon;
	}

	/**
	 * Sets the epsilon parameter for sampling
	 * @param epsilon the epsilon parameter
	 */
	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
	}

	/**
	 * Returns the number of partitions for the datasets
	 * @return the number of partitions
	 */
	public int getNumOfPartition() {
		return numOfPartition;
	}

	/**
	 * Sets the number of partitions for the datasets
	 * @param numOfPartition the number of partitions
	 */
	public void setNumOfPartition(int numOfPartition) {
		this.numOfPartition = numOfPartition;
	}

	/**
	 * Returns curve option: 1 for Hilbert, 2, for z-order and 3 for Gray code curve
	 * @return curve option
	 */
	public int getHOrZOrG() {
		return hOrZOrG;
	}

	/**
	 * Sets curve option: 1 for Hilbert, 2, for z-order and 3 for Gray code curve
	 * @param hOrZOrG curve option
	 */
	public void setHOrZOrG(int hOrZOrG) {
		this.hOrZOrG = hOrZOrG;
	}

	/**
	 * Get the scale vector.
	 * For SWM: hour, timezone, day_of_week, month, weekend, customer_category, customer, day_of_month, customer_monthly
	 * @return scale vector
	 */
	public int[] getScale() {
		return scale;
	}

	/**
	 * Set the scale vector.
	 * For SWM: hour, timezone, day_of_week, month, weekend, customer_category, customer, day_of_month, customer_monthly
	 * @param scale the scale vector
	 */
	public void setScale(int[] scale) {
		this.scale = scale;
	}

	/**
	 * @return
	 */
	public int[][] getShiftVectors() {
		return shiftVectors;
	}

	/**
	 * @param shiftvectors
	 */
	public void setShiftVectors(int[][] shiftvectors) {
		this.shiftVectors = shiftvectors;
	}

	/**
	 * @return
	 */
	public String getHdfsPath() {
		return hdfsPath;
	}

	/**
	 * @param hdfsPath
	 */
	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}

	/**
	 * @return
	 */
/*	public String getLocalPath() {
		return localPath;
	}*/

	/**
	 * @param localPath
	 */
/*	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}*/

	/**
	 * @return
	 */
	public String getSourcesPath() {
		return sourcesPath;
	}

	/**
	 * @param sourcesPath
	 */
	public void setSourcesPath(String sourcesPath) {
		this.sourcesPath = sourcesPath;
	}

	/**
	 * @return
	 */
	public int getClassifyOrRegress() {
		return classifyOrRegress;
	}

	/**
	 * @param classifyOrRegress
	 */
	public void setClassifyOrRegress(int classifyOrRegress) {
		this.classifyOrRegress = classifyOrRegress;
	}

	/**
	 * @return
	 */
	public int getCrossValidationFolds() {
		return crossValidationFolds;
	}

	/**
	 * @param crossValidationFolds
	 */
	public void setCrossValidationFolds(int crossValidationFolds) {
		this.crossValidationFolds = crossValidationFolds;
	}

	/**
	 * @return
	 */
	public boolean isCrossValidation() {
		return crossValidation;
	}

	/**
	 * @param crossValidation
	 */
	public void setCrossValidation(boolean crossValidation) {
		this.crossValidation = crossValidation;
	}

	/**
	 * @return
	 */
	public boolean isGeneticScaleCalc() {
		return geneticScaleCalc;
	}

	/**
	 * @param geneticScaleCalc
	 */
	public void setGeneticScaleCalc(boolean geneticScaleCalc) {
		this.geneticScaleCalc = geneticScaleCalc;
	}

	/**
	 * @return
	 */
	public int[] getScaleDomain() {
		return scaleDomain;
	}

	/**
	 * @param scaleDomain
	 */
	public void setScaleDomain(int[] scaleDomain) {
		this.scaleDomain = scaleDomain;
	}

}
