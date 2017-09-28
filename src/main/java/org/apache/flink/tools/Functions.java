package org.apache.flink.tools;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class contains various functions that
 * are required throughout the code.
 */
public class Functions {

	/**
	 * Method that generates the random shift vectors and stores them to file
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 * @param filename
	 * @param dimension
	 * @param shift
	 * @throws IOException
	 */
	public static void genRandomShiftVectors(String filename, int dimension,
											 int shift) throws IOException {

		Random r = new Random();
		int[][] shiftvectors = new int[shift][dimension];

		// Generate random shift vectors
		for (int i = 0; i < shift; i++) {
			shiftvectors[i] = createShift(dimension, r, true);
		}

		Path pt = new Path(filename);

		FileSystem fs = pt.getFileSystem();
		fs.delete(pt, true);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));

		//File vectorFile = new File(filename);
		//if (!vectorFile.exists()) {
		//	vectorFile.createNewFile();
		//}

		//OutputStream outputStream = new FileOutputStream(filename);
		//Writer writer = new OutputStreamWriter(outputStream);

		for (int j = 0; j < shift; j++) {
			String shiftVector = "";
			for (int k = 0; k < dimension; k++)
				shiftVector += Integer.toString(shiftvectors[j][k]) + " ";
			bw.write(shiftVector + "\n");
		}
		bw.close();
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 * @param num
	 * @return
	 */
	public static String createExtra(int num) {
		if (num < 1)
			return "";

		char[] extra = new char[num];
		for (int i = 0; i < num; i++)
			extra[i] = '0';
		return (new String(extra));
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 * @param dimension
	 * @param rin
	 * @param shift
	 * @return
	 */
	public static int[] createShift(int dimension, Random rin, boolean shift) {
		Random r = rin;
		int[] rv = new int[dimension]; // random vector

		if (shift) {
			for (int i = 0; i < dimension; i++) {
				rv[i] = (Math.abs(r.nextInt(10001)));
			}
		} else {
			for (int i = 0; i < dimension; i++)
				rv[i] = 0;
		}
		return rv;
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 * @param i
	 * @param size
	 * @param sampleRate
	 * @param numOfPartition
	 * @return
	 */
	public static int getEstimatorIndex(int i, int size, double sampleRate,
										int numOfPartition) {
		double iquantile = (i * 1.0 / numOfPartition);
		int orgRank = (int) Math.ceil((iquantile * size));
		int estRank = 0;

		int val1 = (int) Math.floor(orgRank * sampleRate);
		int val2 = (int) Math.ceil(orgRank * sampleRate);

		int est1 = (int) (val1 * (1 / sampleRate));
		int est2 = (int) (val2 * (1 / sampleRate));

		int dist1 = Math.abs(est1 - orgRank);
		int dist2 = Math.abs(est2 - orgRank);

		if (dist1 < dist2)
			estRank = val1;
		else
			estRank = val2;

		return estRank;
	}

	/**
	 *
	 * @param dimension
	 * @return
	 */
	public static int maxDecDigits(int dimension) {
		int max = 32;
		BigInteger maxDec = new BigInteger("1");
		maxDec = maxDec.shiftLeft(dimension * max);
		maxDec.subtract(BigInteger.ONE);
		return maxDec.toString().length();
	}

	/**
	 *
	 * @param dimension
	 * @return
	 */
	public static String maxDecString(int dimension) {
		int max = 32;
		BigInteger maxDec = new BigInteger("1");
		maxDec = maxDec.shiftLeft(dimension * max);
		maxDec.subtract(BigInteger.ONE);
		return maxDec.toString();
	}

	/**
	 * Mutation function for the genetic algorithm
	 * @param element
	 * @param conf
	 * @param step
	 * @return
	 */
	public static int[] mutate(int[] element, ExecConf conf, int step) {
		Random rand = new Random();
		int r = rand.nextInt(conf.getDimension());
		if ((rand.nextDouble() < 0.5) && element[r] > step - 1) {
			element[r] = element[r] - step;
			return element;
		} else if (element[r] < conf.getScaleDomain()[r]) {
			element[r] = element[r] + step;
			return element;
		}
		return element;
	}

	/**
	 * Crossover function for the genetic algorithm
	 * @param element1
	 * @param element2
	 * @param conf
	 * @return
	 */
	public static int[] crossover(int[] element1, int[] element2, ExecConf conf) {
		Random rand = new Random();
		int r = rand.nextInt(((conf.getDimension() - 2) - 1) + 1) + 1;
		int[] result = new int[conf.getDimension()];
		for (int i = 0; i < r; i++) {
			result[i] = element1[i];
		}
		for (int i = r; i < conf.getDimension(); i++) {
			result[i] = element2[i];
		}
		return result;
	}

	/****************************************************
	 ****************** METRICS *************************
	 ****************************************************/
	/**
	 * Accuracy
	 * @param conf
	 * @return
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public static double calculateAccuracy(ExecConf conf)
			throws NumberFormatException, IOException {

		Path pt = new Path(conf.getHdfsPath() + "RClassification");
		FileSystem fs = pt.getFileSystem();
		BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<String, Float> real = new HashMap<>();

		String lineR;
		while ((lineR = rBr.readLine()) != null) {
			String[] partsR = lineR.split(",");
			real.put(partsR[0], Float.parseFloat(partsR[conf.getDimension() + 2]));
		}

		int i = 1;
		String lineResult;
		BufferedReader resultBr = null;
		int total_correct = 0;
		boolean singleFile = false;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "ClassificationResults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(
						new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				pt = new Path(conf.getHdfsPath() + "ClassificationResults");
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
				singleFile = true;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = 0;
				try {
					k = partsResult[0].indexOf(":");
					if (Float.parseFloat(partsResult[3]) == real.get(partsResult[0].substring(0, k))) {
						total_correct++;
					}
				} catch (Exception e) {
					System.out.println("Error in item " + real.get(partsResult[0].substring(0, k)));
				}
			}

			i++;
			resultBr.close();
			if(singleFile) break;
		}

		resultBr.close();
		rBr.close();
		return 100.0 * total_correct / real.size();
	}

	/**
	 * F-Measure
	 * @param conf
	 * @return
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public static double calculateFMeasure(ExecConf conf)
			throws NumberFormatException, IOException {

		Path pt = new Path(conf.getHdfsPath() + "RClassification");
		FileSystem fs = pt.getFileSystem();
		BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<String, Float> real = new HashMap<>();

		String lineR;
		while ((lineR = rBr.readLine()) != null) {
			String[] partsR = lineR.split(",");
			real.put(partsR[0], Float.parseFloat(partsR[conf.getDimension() + 2]));
		}

		double truePositive = 0, falsePositive = 0, falseNegative = 0;
		BufferedReader resultBr;
		String lineResult;
		int i = 1;
		boolean singleFile = false;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "ClassificationResults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				pt = new Path(conf.getHdfsPath() + "ClassificationResults");
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
				singleFile = true;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = 0;
				try {
					k = partsResult[0].indexOf(":");
					if ((Float.parseFloat(partsResult[3]) == 2) && (real.get(partsResult[0].substring(0, k)) == 2)) {
						truePositive++;
					} else if ((Float.parseFloat(partsResult[3]) == 2) && (real.get(partsResult[0].substring(0, k)) ==
							1)) {
						falsePositive++;
					} else if ((Float.parseFloat(partsResult[3]) == 1) && (real.get(partsResult[0].substring(0, k)) ==
							2)) {
						falseNegative++;
					}
				} catch (Exception e) {
					System.out.println("Error in item " + real.get(partsResult[0].substring(0, k)));
				}
			}

			i++;
			resultBr.close();
			if(singleFile) break;
		}

		double precision = truePositive / (truePositive + falsePositive);
		double recall = truePositive / (truePositive + falseNegative);
		double fmeasure = 2 * ((precision * recall) / (precision + recall));
		return fmeasure;
	}

	/**
	 * Root Mean Squared Error (RMSE)
	 * @param conf
	 * @return
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public static double calculateRMSE(ExecConf conf) throws NumberFormatException, IOException {

		Path pt = new Path(conf.getHdfsPath() + "RRegression");
		FileSystem fs = pt.getFileSystem();
		BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<String, Float> real = new HashMap<>();

		String lineR;
		while ((lineR = rBr.readLine()) != null) {
			String[] partsR = lineR.split(",");
			real.put(partsR[0], Float.parseFloat(partsR[conf.getDimension() + 1]));
		}

		BufferedReader resultBr;
		String lineResult;
		int lineCount = 0;
		double totalSquaredError = 0.0;

		// Count the RMSE for the regression itself
		int i = 1;
		boolean singleFile = false;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "RegressionResults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				pt = new Path(conf.getHdfsPath() + "RegressionResults");
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
				singleFile = true;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = partsResult[0].indexOf(":");
				/** For Alicante case study only: Calculate RMSE only for elements that are non-zero
				 This is for the cases that the Regression is performed
				 on the results of classification. The classifier
				 will naturally have mistaken several values. We should not take
				 under consideration those values. Only the correct
				 classification guesses will indicate the regression's performance.
				 Future work: In case of pure regression the following if statement
				 is obsolete and should be removed. **/
				if (real.get(partsResult[0].substring(0, k)) != 0) {
					totalSquaredError += Math.pow((Float.parseFloat(partsResult[3]) - real.get(partsResult[0]
							.substring(0, k))), 2);
					lineCount++;
				}
			}

			i++;
			resultBr.close();
			if(singleFile) break;
		}

        /*
		// Count the RMSE for the final time series
        pt = new Path(conf.getHdfsPath() + "FinalResults");
        fs = pt.getFileSystem();
        resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));

        while ((lineResult = resultBr.readLine()) != null) {
            String[] partsResult = lineResult.split(" +");
            try {totalSquaredError += Math.pow((Float.parseFloat(partsResult[1]) - real.get(partsResult[0])), 2);}
            catch (Exception e) {
                System.out.println();
            }
            lineCount++;
        }
        */

		totalSquaredError = totalSquaredError / lineCount;
		return Math.sqrt(totalSquaredError);
	}

	/**
	 * Coefficient of Determination (R^2)
	 * @param conf
	 * @return
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public static double calculateRSquared(ExecConf conf) throws NumberFormatException, IOException {

		Path pt = new Path(conf.getHdfsPath() + "RRegression");
		FileSystem fs = pt.getFileSystem();
		BufferedReader rBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
		HashMap<String, Float> real = new HashMap<>();

		String lineR;
		double totalReal = 0.0;
		int counter = 0;
		while ((lineR = rBr.readLine()) != null) {
			String[] partsR = lineR.split(",");
			real.put(partsR[0], Float.parseFloat(partsR[conf.getDimension() + 1]));
			totalReal += Float.parseFloat(partsR[conf.getDimension() + 1]);
			counter++;
		}
		double mean = totalReal / counter;

		BufferedReader resultBr;
		String lineResult;
		double totalSumSquares = 0.0;
		double regresSumSquares = 0.0;

		// Count the R^2 for the regression itself
		int i = 1;
		boolean singleFile = false;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "RegressionResults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				pt = new Path(conf.getHdfsPath() + "RegressionResults");
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
				singleFile = true;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = partsResult[0].indexOf(":");
				/** For Alicante case study only: Calculate R^2 only for elements that are non-zero
				 This is for the cases that the Regression is performed
				 on the results of classification. The classifier
				 will naturally have mistaken several values. We should not take
				 under consideration those values. Only the correct
				 classification guesses will indicate the regression's performance.
				 Future work: In case of pure regression the following if statement
				 is obsolete and should be removed. **/
				if (real.get(partsResult[0].substring(0, k)) != 0) {
					regresSumSquares += Math.pow((Float.parseFloat(partsResult[3]) - mean), 2);
					totalSumSquares += Math.pow((real.get(partsResult[0].substring(0, k)) - mean), 2);
				}
			}
			
			i++;
			resultBr.close();
			if(singleFile) break;
		}

        /*
        // Count the R^2 for the final time series
        pt = new Path(conf.getHdfsPath() + "FinalResults");
        fs = pt.getFileSystem();
        resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));

        while ((lineResult = resultBr.readLine()) != null) {
            String[] partsResult = lineResult.split(" +");
            regresSumSquares += Math.pow((Float.parseFloat(partsResult[1]) - mean), 2);
            totalSumSquares += Math.pow((real.get(partsResult[0]) - mean), 2);
        }
        */

		double rSquared = 1 - (regresSumSquares / totalSumSquares);
		return rSquared;
	}

	/****************************************************
	 ************* DAIAD RELATED METHODS ****************
	 ****************************************************/
	
	/**
	 *
	 * @param alicanteFilename
	 * @param conf
	 * @throws IOException
	 */
	public static void createRSDatasets(String filename, ExecConf conf) throws IOException {

		Path rPt = new Path(conf.getHdfsPath() + "R");
		Path sPt = new Path(conf.getHdfsPath() + "S");

		FileSystem fs = rPt.getFileSystem();
		fs.delete(rPt, true);
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

		fs = sPt.getFileSystem();
		fs.delete(sPt, true);
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

		InputStream in = new BufferedInputStream(new FileInputStream(filename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		LineNumberReader lnr = new LineNumberReader(br);
		lnr.skip(Long.MAX_VALUE);
		int noOfLines = lnr.getLineNumber() + 1;

		in = new BufferedInputStream(new FileInputStream(filename));
		br = new BufferedReader(new InputStreamReader(in));
		int counter = 0;
		// Throw first line
		String line;
		br.readLine();
		while ((line = br.readLine()) != null) {
			if (counter < ((9 * noOfLines) / 10)) {
				sBw.write(line + "\n");
			} else {
				rBw.write(line + "\n");
			}
			counter++;
		}

		rBw.close();
		sBw.close();
		br.close();
		lnr.close();
	}


	/**
	 *
	 * @param conf
	 * @throws IOException
	 */
	public static void excludeJune(ExecConf conf) throws IOException {

		Path sPt = new Path(conf.getHdfsPath() + "datasets/S");

		FileSystem fs = sPt.getFileSystem();
		fs.delete(sPt, true);
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

		InputStream in = new BufferedInputStream(new FileInputStream
				("/Users/whashnez/Programming/FML_kNN_Local/datasets/S_before_June_removal"));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		// Throw first line
		String line;
		br.readLine();
		while ((line = br.readLine()) != null) {
			String parts[] = line.split(",");
			if (parts[1].substring(0, 4).equals("2016") && parts[1].substring(5, 7).equals("06") ||
					parts[0].equals("I15FA102475U") ||
					parts[0].equals("D14IA022973") ||
					parts[0].equals("I14FA088937") ||
					parts[0].equals("I14FA069951") ||
					parts[0].equals("D15IA001749") ||
					parts[0].equals("D12NA073056") ||
					parts[0].equals("I13FA056137")) {
				continue;
			} else {
				sBw.write(line + "\n");
			}
		}

		sBw.close();
		br.close();
	}

	/**
	 * This method generates a specific query dataset R
	 * It will read SWM ids from the S dataset and create
	 * a query for each of them for a specific period
	 * @param conf
	 * @throws IOException
	 */
	public static void createQuery(ExecConf conf) throws IOException {

		String line;
		String newline;
		Path rPt = new Path(conf.getHdfsPath() + "datasets/R");
		Path sPt = new Path(conf.getHdfsPath() + "datasets/S");

		FileSystem fs = rPt.getFileSystem();
		fs.delete(rPt, true);
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

		fs = sPt.getFileSystem();
		BufferedReader sBr = new BufferedReader(new InputStreamReader(fs.open(sPt)));

		String currentSWM = "";
		while ((line = sBr.readLine()) != null) {
			String[] row = line.split(",");
			if (!row[0].equals(currentSWM)) {
				for (int day = 1; day < 31; day++) {
					for (int hour = 0; hour < 24; hour++) {
						newline = row[0] + ",2016-06-" + String.format("%02d", day) + " " + String.format("%02d",
								hour) + ":00\n";
						rBw.write(newline);
					}
				}
			}
			currentSWM = row[0];
		}

		rBw.close();
		sBr.close();
	}

	/**
	 *
	 * @param alicanteFilename
	 * @param conf
	 * @throws IOException
	 */
	public static void createDatasetsSpecific(String alicanteFilename, ExecConf conf) throws IOException {

		Path rPt = new Path(conf.getHdfsPath() + "datasets/RClassification");
		Path sPt = new Path(conf.getHdfsPath() + "datasets/SClassification");

		FileSystem fs = rPt.getFileSystem();
		fs.delete(rPt, true);
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

		fs = sPt.getFileSystem();
		fs.delete(sPt, true);
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

		InputStream in = new BufferedInputStream(new FileInputStream(alicanteFilename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		// Throw first line
		String line;
		br.readLine();
		while ((line = br.readLine()) != null) {
			String parts[] = line.split(",");
			if (//parts[0].substring(0,11).equals("C11UA487383") &&
					parts[0].substring(12, 16).equals("2016") &&
							parts[0].substring(17, 19).equals("04") &&
							//Integer.parseInt(parts[0].substring(20,22)) >= 30 &&
							Integer.parseInt(parts[0].substring(20, 22)) == 23) {
				rBw.write(line + "\n");
			} else {
				sBw.write(line + "\n");
			}
		}

		rBw.close();
		sBw.close();
		br.close();

	}

	/**
	 *
	 * @param alicanteFilename
	 * @param conf
	 * @throws IOException
	 */
	public static void createDatasetsSpecificInitial(String alicanteFilename, ExecConf conf) throws IOException {

		Path rPt = new Path(conf.getHdfsPath() + "datasets/R");
		Path sPt = new Path(conf.getHdfsPath() + "datasets/S");

		FileSystem fs = rPt.getFileSystem();
		fs.delete(rPt, true);
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

		fs = sPt.getFileSystem();
		fs.delete(sPt, true);
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

		InputStream in = new BufferedInputStream(new FileInputStream(alicanteFilename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		// Throw first line
		String line;
		br.readLine();
		while ((line = br.readLine()) != null) {
			String parts2[] = line.split(",");
			String parts[] = parts2[0].split("_");
			if (//parts[0].substring(0,11).equals("C11UA487383") &&
					parts[1].substring(0, 4).equals("2014") &&
							parts[1].substring(5, 7).equals("06") &&
							Integer.parseInt(parts[1].substring(8, 10)) >= 16 &&
							Integer.parseInt(parts[1].substring(8, 10)) <= 30) {
				rBw.write(line + "\n");
			} else {
				sBw.write(line + "\n");
			}
		}

		rBw.close();
		sBw.close();
		br.close();

	}

	/**
	 *
	 * @param alicanteFilename
	 * @param conf
	 * @throws IOException
	 */
	public static void createDatasetsClassification(String alicanteFilename, ExecConf conf) throws IOException {

		Path rPt = new Path(conf.getHdfsPath() + "datasets/RClassification");
		Path sPt = new Path(conf.getHdfsPath() + "datasets/SClassification");

		FileSystem fs = rPt.getFileSystem();
		fs.delete(rPt, true);
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

		fs = sPt.getFileSystem();
		fs.delete(sPt, true);
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

		InputStream in = new BufferedInputStream(new FileInputStream(alicanteFilename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		LineNumberReader lnr = new LineNumberReader(br);
		lnr.skip(Long.MAX_VALUE);
		int noOfLines = lnr.getLineNumber() + 1;

		in = new BufferedInputStream(new FileInputStream(alicanteFilename));
		br = new BufferedReader(new InputStreamReader(in));
		int counter = 0;
		// Throw first line
		String line;
		br.readLine();
		while ((line = br.readLine()) != null) {
			if (counter < ((9 * noOfLines) / 10)) {
				sBw.write(line + "\n");
			} else {
				rBw.write(line + "\n");
			}
			counter++;
		}

		rBw.close();
		sBw.close();
		br.close();
		lnr.close();
	}

	/**
	 *
	 * @param conf
	 * @throws IOException
	 */
	public static void createDatasetsRegression(ExecConf conf) throws IOException {

		Path rPt = new Path(conf.getHdfsPath() + "RRegression");
		Path sPt = new Path(conf.getHdfsPath() + "SRegression");
		Path unifiedNonZero = new Path(conf.getHdfsPath() + "UnifiedNonZeroDataset");

		FileSystem fs = rPt.getFileSystem();
		fs.delete(rPt, true);
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(fs.create(rPt, true)));

		fs = sPt.getFileSystem();
		fs.delete(sPt, true);
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(fs.create(sPt, true)));

		fs = unifiedNonZero.getFileSystem();
		fs.delete(unifiedNonZero, true);
		BufferedWriter unifiedNonZeroWriter = new BufferedWriter(new OutputStreamWriter(fs.create(unifiedNonZero,
				true)));

		Path pt;
		int i = 1;
		BufferedReader resultBr;
		String lineResult;
		HashMap<String, Integer> results = new HashMap<>();

		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "ClassificationResults/" + i);
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				pt = new Path(conf.getHdfsPath() + "ClassificationResults");
				fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));

				while ((lineResult = resultBr.readLine()) != null) {
					String[] partsResult = lineResult.split(" +");
					if (Float.parseFloat(partsResult[3]) == 2) {
						int k = partsResult[0].indexOf(":");
						results.put(partsResult[0].substring(0, k), 1);
					}
				}
				break;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				if (Float.parseFloat(partsResult[3]) == 2) {
					int k = partsResult[0].indexOf(":");
					results.put(partsResult[0].substring(0, k), 1);
				}
			}

			i++;
			resultBr.close();
		}

		pt = new Path(conf.getHdfsPath() + "RClassification");
		fs = pt.getFileSystem();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line;

		while ((line = br.readLine()) != null) {
			String[] partsLine = line.split(",+");
			if (results.get(partsLine[0]) != null) {
				// Write non-zero consumption elements to the unified file for cross-validation reasons
				//if(Float.parseFloat(partsLine[conf.getValueDim()+2]) == 2) {
				rBw.write(line + "\n");
				unifiedNonZeroWriter.write(line + "\n");
				//}
			}
		}

		pt = new Path(conf.getHdfsPath() + "SClassification");
		fs = pt.getFileSystem();
		br = new BufferedReader(new InputStreamReader(fs.open(pt)));

		while ((line = br.readLine()) != null) {
			String[] partsLine = line.split(",+");
			if (Float.parseFloat(partsLine[conf.getDimension() + 2]) == 2) {
				sBw.write(line + "\n");
				// Write non-zero consumption elements to the unified file for cross-validation reasons
				unifiedNonZeroWriter.write(line + "\n");
			}
		}

		br.close();
		rBw.close();
		sBw.close();
		unifiedNonZeroWriter.close();
	}

	/**
	 * This method produces the final time-series
	 * @param conf
	 * @throws IOException
	 */
	public static void produceFinalDataset(ExecConf conf) throws IOException {

		Path pt;
		int i = 1;
		BufferedReader resultBr;
		String lineResult;
		HashMap<String, Float> results = new HashMap<>();

		boolean flag = true;
		while (true) {
			try {
				pt = new Path(conf.getHdfsPath() + "RegressionResults/" + i);
				FileSystem fs = pt.getFileSystem();
				resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));
			} catch (Exception e) {
				// Try to read from a single file
				break;
			}

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = partsResult[0].indexOf(":");
				results.put(partsResult[0].substring(0, k), Float.parseFloat(partsResult[3]));
			}

			i++;
			resultBr.close();
			flag = false;
		}

		if (flag) {
			pt = new Path(conf.getHdfsPath() + "RegressionResults");
			FileSystem fs = pt.getFileSystem();
			resultBr = new BufferedReader(new InputStreamReader(fs.open(pt)));

			while ((lineResult = resultBr.readLine()) != null) {
				String[] partsResult = lineResult.split(" +");
				int k = partsResult[0].indexOf(":");
				results.put(partsResult[0].substring(0, k), Float.parseFloat(partsResult[3]));
			}
		}

		Path rPt = new Path(conf.getHdfsPath() + "RClassification");
		Path resPt = new Path(conf.getHdfsPath() + "FinalResults");
		FileSystem fsR = rPt.getFileSystem();
		FileSystem fsRes = rPt.getFileSystem();
		BufferedReader rBr = new BufferedReader(new InputStreamReader(fsR.open(rPt)));
		BufferedWriter resBw = new BufferedWriter(new OutputStreamWriter(fsRes.create(resPt, true)));

		String lineR;
		while ((lineR = rBr.readLine()) != null) {
			String[] partsR = lineR.split(",");
			if (results.get(partsR[0]) != null) {
				partsR = partsR[0].split("_");
				partsR[1].replace("-", "/");
				resBw.write(partsR[0] + ";" + partsR[1].substring(0, 10) + " " + partsR[1].substring(11, 13) + ":00:00;" + results.get(partsR[0] + "_" + partsR[1]) + "\n");
			} else {
				resBw.write(partsR[0] + ";" + partsR[1] + ";0.0\n");
			}
		}

		resBw.close();
		rBr.close();
	}

	/**
	 * This method creates datasets for cross-validation
	 * @param alicanteFilename
	 * @param conf
	 * @param fold
	 * @param requestedPartition
	 * @throws IOException
	 */
	public static void createDatasetsCrossValid(
			String alicanteFilename, ExecConf conf, int fold,
			int requestedPartition) throws IOException {

		InputStream in = new BufferedInputStream(new FileInputStream(alicanteFilename));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		LineNumberReader lnr = new LineNumberReader(br);
		lnr.skip(Long.MAX_VALUE);
		int datasetSize = (lnr.getLineNumber() + 1);
		int partitionSize = datasetSize / fold;

		// Throw first line
		in = new BufferedInputStream(new FileInputStream(alicanteFilename));
		br = new BufferedReader(new InputStreamReader(in));
		String line;
		br.readLine();

		Path rPt = null;
		Path sPt = null;
		if (conf.getClassifyOrRegress() == 1) {
			rPt = new Path(conf.getHdfsPath() + "datasets/RClassification");
			sPt = new Path(conf.getHdfsPath() + "datasets/SClassification");
		} else if (conf.getClassifyOrRegress() == 2) {
			rPt = new Path(conf.getHdfsPath() + "datasets/RRegression");
			sPt = new Path(conf.getHdfsPath() + "datasets/SRegression");
		}

		FileSystem fs = rPt.getFileSystem();
		fs.delete(rPt, true);
		BufferedWriter rBw = new BufferedWriter(new OutputStreamWriter(
				fs.create(rPt, true)));

		fs = sPt.getFileSystem();
		fs.delete(sPt, true);
		BufferedWriter sBw = new BufferedWriter(new OutputStreamWriter(
				fs.create(sPt, true)));

		int counter = 0;
		int entryPoint = requestedPartition * partitionSize;
		int exitPoint = (requestedPartition + 1) * partitionSize;
		while ((line = br.readLine()) != null) {
			if ((counter >= entryPoint) && (counter < exitPoint)) {
				rBw.write(line + "\n");
				counter++;
				continue;
			}
			sBw.write(line + "\n");
			counter++;
		}

		lnr.close();
		rBw.close();
		sBw.close();
		br.close();
	}

	/**
	 * SWM data only: This method performs feature extraction and outlier removal on datasets R (query) and S (train)
	 * containing only the consumption data and the time stamps. The output files are ready to be used by the
	 * classifier.
	 *
	 * @param sourcesPath
	 */
	public static void featureExtraction(ExecConf conf) throws IOException, ParseException {

		String R = conf.getHdfsPath() + "R";
		String Rclass = conf.getHdfsPath() + "RClassification";
		String S = conf.getHdfsPath() + "S";
		String Sclass = conf.getHdfsPath() + "SClassification";
		BufferedReader br;
		BufferedWriter bw;
		String line, newline = null;
		String delimiter = ";";

		// ### Read the data codes ### //
		Path pt = new Path(conf.getHdfsPath() + "outliers.txt");
		FileSystem fs = pt.getFileSystem();
		BufferedReader jbrOutliers = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jOutliers = jbrOutliers.readLine();
		jbrOutliers.close();
		pt = new Path(conf.getHdfsPath() + "hour_codes.txt");
		fs = pt.getFileSystem();
		BufferedReader jbrHourCodes = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jHourCodes = jbrHourCodes.readLine();
		jbrHourCodes.close();
		pt = new Path(conf.getHdfsPath() + "timezone_codes.txt");
		fs = pt.getFileSystem();
		BufferedReader jbrTimezoneCodes = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jTimezoneCodes = jbrTimezoneCodes.readLine();
		jbrTimezoneCodes.close();
		pt = new Path(conf.getHdfsPath() + "day_codes.txt");
		fs = pt.getFileSystem();
		BufferedReader jbrDayCodes = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jDayCodes = jbrDayCodes.readLine();
		jbrDayCodes.close();
		pt = new Path(conf.getHdfsPath() + "month_codes.txt");
		fs = pt.getFileSystem();
		BufferedReader jbrMonthCodes = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jMonthCodes = jbrMonthCodes.readLine();
		jbrMonthCodes.close();
		pt = new Path(conf.getHdfsPath() + "season_codes.txt");
		fs = pt.getFileSystem();
		BufferedReader jbrSeasonCodes = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jSeasonCodes = jbrSeasonCodes.readLine();
		jbrSeasonCodes.close();
		pt = new Path(conf.getHdfsPath() + "customer_category_codes.txt");
		fs = pt.getFileSystem();
		BufferedReader jbrCustomerCatCodes = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jCustomerCatCodes = jbrCustomerCatCodes.readLine();
		jbrCustomerCatCodes.close();
		pt = new Path(conf.getHdfsPath() + "customer_codes.txt");
		fs = pt.getFileSystem();
		BufferedReader jbrCustomerCodes = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jCustomerCodes = jbrCustomerCodes.readLine();
		jbrCustomerCodes.close();
		pt = new Path(conf.getHdfsPath() + "customer_category_monthly_codes.txt");
		fs = pt.getFileSystem();
		BufferedReader jbrCustomerMonCodes = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jCustomerMonCodes = jbrCustomerMonCodes.readLine();
		jbrCustomerMonCodes.close();
		pt = new Path(conf.getHdfsPath() + "day_of_month_codes.txt");
		fs = pt.getFileSystem();
		BufferedReader jbrDayOfMonthCodes = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String jDayOfMonthCodes = jbrDayOfMonthCodes.readLine();
		jbrDayOfMonthCodes.close();
				
		JSONObject outliers = new JSONObject(jOutliers);
		JSONObject hourCodes = new JSONObject(jHourCodes);
		JSONObject timezoneCodes = new JSONObject(jTimezoneCodes);
		JSONObject dayCodes = new JSONObject(jDayCodes);
		JSONObject monthCodes = new JSONObject(jMonthCodes);
		JSONObject seasonCodes = new JSONObject(jSeasonCodes);
		JSONObject customerCatCodes = new JSONObject(jCustomerCatCodes);
		JSONObject customerCodes = new JSONObject(jCustomerCodes);
		JSONObject customerMonCodes = new JSONObject(jCustomerMonCodes);
		JSONObject dayOfMonthCodes = new JSONObject(jDayOfMonthCodes);

		// Do not take under consideration the outliers in the S dataset
		// ##### R Dataset ##### //
		Path rPt = new Path(R);
		Path clrPt = new Path(Rclass);
		FileSystem fsR = rPt.getFileSystem();
		FileSystem fsClr = clrPt.getFileSystem();
		br = new BufferedReader(new InputStreamReader(fsR.open(rPt)));
		bw = new BufferedWriter(new OutputStreamWriter(fsClr.create(clrPt, true)));
		String prev = null;
		while ((line = br.readLine()) != null) {

            /* ### Create Header ### */
			String[] row = line.split(delimiter);
			if (row[0] == "household_id") {
				newline = "householdId_datetime,hour,timezone,day_of_week,month,season,weekend,customer_category," +
						"customer,customer_monthly,day_of_month";
				bw.write(newline);
				continue;
			}

            /* ### Create Keys ### */
			newline = row[0] + "_" + row[1].substring(0, 10) + "-" + row[1].substring(11, 13);
			//newline = row[0] + "_" + row[1].substring(0, 10) + "-" + row[1].substring(11, 13) + "," + row[3];

            /* ### Create Hours ### */
			newline = newline + "," + hourCodes.get(row[1].substring(11, 13));

            /* ### Create Timezones ### */
			int dayOfWeek = getDayOfWeek(row[1].substring(0, 10));
			if (dayOfWeek == 6 || dayOfWeek == 7) {
				if (Integer.parseInt(row[1].substring(11, 13)) > 0 && Integer.parseInt(row[1].substring(11, 13)) < 6)
					newline = newline + "," + timezoneCodes.get("1");
				if (Integer.parseInt(row[1].substring(11, 13)) == 0 || Integer.parseInt(row[1].substring(11, 13)) ==
						6 ||
						Integer.parseInt(row[1].substring(11, 13)) == 7 || Integer.parseInt(row[1].substring(11, 13))
						== 22 ||
						Integer.parseInt(row[1].substring(11, 13)) == 23)
					newline = newline + "," + timezoneCodes.get("2");
				if (Integer.parseInt(row[1].substring(11, 13)) == 8 || Integer.parseInt(row[1].substring(11, 13)) >
						12 &&
						Integer.parseInt(row[1].substring(11, 13)) < 22)
					newline = newline + "," + timezoneCodes.get("3");
				if (Integer.parseInt(row[1].substring(11, 13)) > 8 && Integer.parseInt(row[1].substring(11, 13)) < 13)
					newline = newline + "," + timezoneCodes.get("4");
			} else {
				if (Integer.parseInt(row[1].substring(11, 13)) > 0 && Integer.parseInt(row[1].substring(11, 13)) < 5)
					newline = newline + "," + timezoneCodes.get("5");
				if (Integer.parseInt(row[1].substring(11, 13)) == 0 || Integer.parseInt(row[1].substring(11, 13)) ==
						5 ||
						Integer.parseInt(row[1].substring(11, 13)) == 6 || Integer.parseInt(row[1].substring(11, 13))
						== 22 ||
						Integer.parseInt(row[1].substring(11, 13)) == 23)
					newline = newline + "," + timezoneCodes.get("6");
				if (Integer.parseInt(row[1].substring(11, 13)) > 6 && Integer.parseInt(row[1].substring(11, 13)) < 22)
					newline = newline + "," + timezoneCodes.get("7");
			}

            /* ### Create Day of Week ### */
			newline = newline + "," + dayCodes.get(Integer.toString(dayOfWeek));

            /* ### Create Month ### */
			newline = newline + "," + monthCodes.get(row[1].substring(3, 5));

            /* ### Create Season ### */
            if (Integer.parseInt(row[1].substring(3, 5)) > 11 || Integer.parseInt(row[1].substring(3, 5)) < 3)
                newline = newline + "," + seasonCodes.get("1");
            if (Integer.parseInt(row[1].substring(3, 5)) > 2 && Integer.parseInt(row[1].substring(3, 5)) < 6)
                newline = newline + "," + seasonCodes.get("2");
            if (Integer.parseInt(row[1].substring(3, 5)) > 5 && Integer.parseInt(row[1].substring(3, 5)) < 9)
                newline = newline + "," + seasonCodes.get("3");
            if (Integer.parseInt(row[1].substring(3, 5)) > 8 && Integer.parseInt(row[1].substring(3, 5)) < 12)
                newline = newline + "," + seasonCodes.get("4");

            /* ### Create Weekend ### */
			if (dayOfWeek == 6 || dayOfWeek == 7)
				newline = newline + "," + "2";
			else
				newline = newline + "," + "1";

            /* ### Create Customer Category ### */
			boolean done = false;
			for (String key : customerCatCodes.keySet()) {
				JSONArray array = (JSONArray) customerCatCodes.get(key);
				ArrayList<Object> list = new ArrayList<>();
				if (array != null) {
					for (int i = 0; i < array.length(); i++) {
						list.add(array.get(i).toString());
					}
				}
				if (list.contains(row[0])) {
					newline = newline + "," + Integer.toString(Integer.parseInt(key) + 1);
					done = true;
				}
			}
			if(!done) {
				if (!row[0].equals(prev)) System.out.println("User " + row[0] + " is ignored");
				prev = row[0];
				continue;
			}

            /* ### Create Customer ### */
			// If the user does not exist on the list, discard
			JSONObject jObject = (JSONObject) customerCodes.get(row[1].substring(11, 13));
			try {
				newline = newline + "," + jObject.get(row[0]);
			} catch (JSONException e) {
				if (!row[0].equals(prev)) System.out.println("User " + row[0] + " is ignored");
				prev = row[0];
				continue;
			}

            /* ### Create Customer Category Monthly ### */
			// If the user does not exist on the list, discard
			try {
				newline = newline + "," + customerMonCodes.get(row[0]);
			} catch (JSONException e) {
				if (!row[0].equals(prev)) System.out.println("User " + row[0] + " is ignored");
				prev = row[0];
				continue;
			}
			
			/* ### Create Day of Month ### */
			int dayOfMonth = getDayOfMonth(row[1].substring(0, 10));
			newline = newline + "," + dayOfMonthCodes.get(Integer.toString(dayOfMonth)) + ",-1,-1\n";
			//newline = newline + "," + dayOfMonthCodes.get(Integer.toString(dayOfMonth));

            /* ### Create Consumption or Not ###
            if (Double.parseDouble(row[2]) > 0.0)
                newline = newline + "," + "2";
            else
                newline = newline + "," + "1";

            /* ### Bring consumption in front
            String[] newlineArray = newline.split(",");
            newline = newlineArray[0] + "," + newlineArray[2] + "," + newlineArray[3] + "," +
					newlineArray[4] + "," + newlineArray[5] + "," + newlineArray[6] + "," + newlineArray[7] + "," +
					newlineArray[8] + "," + newlineArray[9] + "," + newlineArray[10] + "," + newlineArray[11] +
					"," + newlineArray[1] + "," + newlineArray[12] + "\n";
			*/

			bw.write(newline);
		}
		bw.close();

		// ##### S Dataset ##### //
		Path sPt = new Path(S);
		Path clsPt = new Path(Sclass);
		FileSystem fsS = sPt.getFileSystem();
		FileSystem fsCls = clsPt.getFileSystem();
		br = new BufferedReader(new InputStreamReader(fsS.open(sPt)));
		bw = new BufferedWriter(new OutputStreamWriter(fsCls.create(clsPt, true)));
		prev = null;
		while ((line = br.readLine()) != null) {

            /* ### Create Header ### */
			String[] row = line.split(delimiter);
			if (row[0] == "household_id") {
				newline = "householdId_datetime,hour,timezone,day_of_week,month,season,weekend,customer_category," +
						"customer,customer_monthly,day_of_month,consumption,consmption_or_not";
				bw.write(newline);
				continue;
			}

            /* ### If outlier, continue ### */
			JSONArray array = (JSONArray) outliers.get("0");
			ArrayList<String> list = new ArrayList<>();
			if (array != null) {
				for (int i = 0; i < array.length(); i++) {
					list.add(array.get(i).toString());
				}
			}
			if (list.contains(row[0]))
				continue;

            /* ### Create Keys ### */
			newline = row[0] + "_" + row[1].substring(0, 10) + "-" + row[1].substring(11, 13) + "," + row[3];

            /* ### Create Hours ### */
			newline = newline + "," + hourCodes.get(row[1].substring(11, 13));

            /* ### Create Timezones ### */
			int dayOfWeek = getDayOfWeek(row[1].substring(0, 10));
			if (dayOfWeek == 6 || dayOfWeek == 7) {
				if (Integer.parseInt(row[1].substring(11, 13)) > 0 && Integer.parseInt(row[1].substring(11, 13)) < 6)
					newline = newline + "," + timezoneCodes.get("1");
				if (Integer.parseInt(row[1].substring(11, 13)) == 0 || Integer.parseInt(row[1].substring(11, 13)) == 6 ||
						Integer.parseInt(row[1].substring(11, 13)) == 7 || Integer.parseInt(row[1].substring(11, 13)) == 22 ||
						Integer.parseInt(row[1].substring(11, 13)) == 23)
					newline = newline + "," + timezoneCodes.get("2");
				if (Integer.parseInt(row[1].substring(11, 13)) == 8 || Integer.parseInt(row[1].substring(11, 13)) >	12 &&
						Integer.parseInt(row[1].substring(11, 13)) < 22)
					newline = newline + "," + timezoneCodes.get("3");
				if (Integer.parseInt(row[1].substring(11, 13)) > 8 && Integer.parseInt(row[1].substring(11, 13)) < 13)
					newline = newline + "," + timezoneCodes.get("4");
			} else {
				if (Integer.parseInt(row[1].substring(11, 13)) > 0 && Integer.parseInt(row[1].substring(11, 13)) < 5)
					newline = newline + "," + timezoneCodes.get("5");
				if (Integer.parseInt(row[1].substring(11, 13)) == 0 || Integer.parseInt(row[1].substring(11, 13)) == 5 ||
						Integer.parseInt(row[1].substring(11, 13)) == 6 || Integer.parseInt(row[1].substring(11, 13)) == 22 ||
						Integer.parseInt(row[1].substring(11, 13)) == 23)
					newline = newline + "," + timezoneCodes.get("6");
				if (Integer.parseInt(row[1].substring(11, 13)) > 6 && Integer.parseInt(row[1].substring(11, 13)) < 22)
					newline = newline + "," + timezoneCodes.get("7");
			}

            /* ### Create Day of Week ### */
			newline = newline + "," + dayCodes.get(Integer.toString(dayOfWeek));

            /* ### Create Month ### */
			newline = newline + "," + monthCodes.get(row[1].substring(3, 5));

            /* ### Create Season ### */
            if (Integer.parseInt(row[1].substring(3, 5)) > 11 || Integer.parseInt(row[1].substring(3, 5)) < 3)
                newline = newline + "," + seasonCodes.get("1");
            if (Integer.parseInt(row[1].substring(3, 5)) > 2 && Integer.parseInt(row[1].substring(3, 5)) < 6)
                newline = newline + "," + seasonCodes.get("2");
            if (Integer.parseInt(row[1].substring(3, 5)) > 5 && Integer.parseInt(row[1].substring(3, 5)) < 9)
                newline = newline + "," + seasonCodes.get("3");
            if (Integer.parseInt(row[1].substring(3, 5)) > 8 && Integer.parseInt(row[1].substring(3, 5)) < 12)
                newline = newline + "," + seasonCodes.get("4");

            /* ### Create Weekend ### */
			if (dayOfWeek == 6 || dayOfWeek == 7)
				newline = newline + "," + "2";
			else
				newline = newline + "," + "1";

            /* ### Create Customer Category ### */
			boolean done = false;
			for (String key : customerCatCodes.keySet()) {
				array = (JSONArray) customerCatCodes.get(key);
				list = new ArrayList<>();
				if (array != null) {
					for (int i = 0; i < array.length(); i++) {
						list.add(array.get(i).toString());
					}
				}
				if (list.contains(row[0])) {
					newline = newline + "," + Integer.toString(Integer.parseInt(key) + 1);
					done = true;
					break;
				}
			}
			if(!done) {
				if (!row[0].equals(prev)) System.out.println("User " + row[0] + " is ignored");
				prev = row[0];
				continue;
			}

			// If the user does not exist on the list, discard
			JSONObject jObject = (JSONObject) customerCodes.get(row[1].substring(11, 13));
			try {
				newline = newline + "," + jObject.get(row[0]);
			} catch (JSONException e) {
				if (!row[0].equals(prev)) System.out.println("User " + row[0] + " is ignored");
				prev = row[0];
				continue;
			}

            /* ### Create Customer Category Monthly ### */
			// If the user does not exist on the list, discard
			
			try {
				newline = newline + "," + customerMonCodes.get(row[0]);
			} catch (JSONException e) {
				if (!row[0].equals(prev)) System.out.println("User " + row[0] + " is ignored");
				prev = row[0];
				continue;
			}

            /* ### Create Day of Month ### */
			int dayOfMonth = getDayOfMonth(row[1].substring(0, 10));
			newline = newline + "," + dayOfMonthCodes.get(Integer.toString(dayOfMonth));

            /* ### Create Consumption or Not ### */
			if (Double.parseDouble(row[2]) > 0.0)
				newline = newline + "," + "2";
			else
				newline = newline + "," + "1";

            /* ### Bring consumption in front */
			String[] newlineArray = newline.split(",");
			String finalNewline;
			finalNewline = newlineArray[0] + "," + newlineArray[2] + "," + newlineArray[3] + "," +
					newlineArray[4] + "," + newlineArray[5] + "," + newlineArray[6] + "," + newlineArray[7] + "," +
					newlineArray[8] + "," + newlineArray[9] + "," + newlineArray[10] + "," + newlineArray[11] +
					"," + newlineArray[1] + "," + newlineArray[12] + "\n";

			bw.write(finalNewline);
		}
		bw.close();

	}

	/**
	 *
	 * @param d
	 * @return
	 * @throws ParseException
	 */
	private static int getDayOfWeek(String d) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("dd/mm/yyyy");
		Date date = formatter.parse(d);
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int dayOfWeek = c.get(Calendar.DAY_OF_WEEK) - 1;
		if (dayOfWeek == 0)
			return 7;
		else return dayOfWeek;
	}

	/**
	 *
	 * @param d
	 * @return
	 * @throws ParseException
	 */
	private static int getDayOfMonth(String d) throws ParseException {
		DateFormat formatter = new SimpleDateFormat("dd/mm/yyyy");
		Date date = formatter.parse(d);
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int dayOfMonth = c.get(Calendar.DAY_OF_MONTH);
		return dayOfMonth;
	}

}