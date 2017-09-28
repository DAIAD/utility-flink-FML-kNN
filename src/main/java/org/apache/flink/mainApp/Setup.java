package org.apache.flink.mainApp;

import org.apache.flink.tools.ExecConf;
import org.apache.flink.tools.Functions;

import java.util.Random;

/**
 * This class is where the application starts and is setup
 * to run in one of the three separate modes (normal, cross-validation, scale genetic-optimization)
 */
public class Setup {

	/**
	 * The main method of the application
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
		FML_kNN app = new FML_kNN();
		ExecConf conf = new ExecConf();

		// Read arguments and configure
		if (args.length > 0) {
			try {
				conf.setKnn(Integer.parseInt(args[0]));
				conf.setShift(Integer.parseInt(args[1]));
				conf.setDimension(Integer.parseInt(args[2]));
				conf.setNoOfClasses(Integer.parseInt(args[3]));
				conf.setEpsilon(Double.parseDouble(args[4]));
				conf.setNumOfPartition(Integer.parseInt(args[5]));
				conf.setHOrZOrG(Integer.parseInt(args[6]));
				conf.setClassifyOrRegress(Integer.parseInt(args[7]));
				conf.setGeneticScaleCalc(Boolean.parseBoolean(args[8]));
				if (args.length == 10) {
					conf.setHdfsPath(args[9]);
					//conf.setLocalPath(args[9]);
				}
				if (args.length == 11) {
					conf.setHdfsPath(args[9]);
					//conf.setLocalPath(args[10]);
				}
			} catch (Exception e) {
				System.out.println("Error! Please check arguments.");
				System.exit(0);
			}
		}

		/****************************************************
		 ************** GENETIC ALGORITHM *******************
		 ****************************************************/
		// This part is where the genetic algorithm is executed, in order to optimize the scale
		if (conf.isGeneticScaleCalc()) {
			Random rand = new Random();

			/** Genetic optimization algorithm configuration **/
			int populationSize = 20;
			int step = 2;
			double mutationProb = 0.3;
			double eliteFraction = 0.3;
			int iterations = 10;
			int scoreElement = 0; // 0: Accuracy, 1: F-Measure, 0: R^2, 1: RMSE
			/** ############################################ **/

			// Build the initial population
			int[][] population = new int[populationSize][conf.getDimension()];
			for (int i = 0; i < populationSize; i++) {
				int[] tmpScale = new int[conf.getDimension()];
				for (int j = 0; j < conf.getDimension(); j++) {
					if (j < 6) tmpScale[j] = rand.nextInt(conf.getScaleDomain()[j] + 1);
					else tmpScale[j] = rand.nextInt((conf.getScaleDomain()[j] - 10) + 1) + 10;
				}
				population[i] = tmpScale;
			}

			// Determine the number of elit elements
			int topElite = (int) Math.round(eliteFraction * (double) populationSize);

			/** Main algorithm loop **/
			for (int i = 0; i < iterations; i++) {
				double[] scores = new double[populationSize];
				for (int j = 0; j < populationSize; j++) {
					boolean retry = false;
					if (retry == true) j--;
					conf.setScale(population[j]);
					try {
						scores[j] = app.execute(conf)[scoreElement];
					} catch (Exception e) {
						System.out.println("Caught exception: ");
						e.printStackTrace();
						System.out.println("Retrying ...");
						retry = true;
					}
				}

				// Buuble sort population according to their scores
				for (int n = 0; n < populationSize; n++) {
					for (int m = 0; m < (populationSize - 1) - n; m++) {
						if (scores[m] < scores[m + 1]) {
							double swap1 = scores[m];
							scores[m] = scores[m + 1];
							scores[m + 1] = swap1;
							int[] swap2 = population[m];
							population[m] = population[m + 1];
							population[m + 1] = swap2;
						}
					}
				}

				int[][] nextGen = new int[populationSize][conf.getDimension()];
				double[] nextGenScores = new double[populationSize];

				// Add elites to the next generation
				for (int k = 0; k < topElite; k++) {
					nextGen[k] = population[k];
					nextGenScores[k] = scores[k];
				}

				// Add the rest of the elements to the next generation
				for (int j = topElite; j < populationSize; j++) {
					// Perform mutation
					if (rand.nextDouble() < mutationProb) {
						int randomEliteElement = rand.nextInt(topElite);
						nextGen[j] = Functions.mutate(nextGen[randomEliteElement], conf, step);
					}
					// Perform crossover
					else {
						int randomEliteElement1 = rand.nextInt(topElite);
						int randomEliteElement2 = rand.nextInt(topElite);
						nextGen[j] = Functions.crossover(nextGen[randomEliteElement1], nextGen[randomEliteElement2],
								conf);
					}
				}

				// Print current best element with score
				for (int l = 0; l < conf.getDimension(); l++) {
					System.out.print(nextGen[0][l] + ", ");
				}
				System.out.print(nextGenScores[0] + "\n");

				// Make next generation current
				population = nextGen;
			}
			System.out.println("Genetic algorithm execution finished!");
		}

		/**
		 * In case genetic algorithm execution is not required (normal/cross-validation mode), just print the results
 		 */
		else {

			//double[] results = app.execute(conf);
			app.execute(conf);
           
			/*
            if (conf.getClassifyOrRegress() == 1) {
                System.out.println("Final accuracy: " + results[0]);
                System.out.println("Final F-measure: " + results[1]);
            }

            else if (conf.getClassifyOrRegress() == 2) {
                System.out.println("Final Coefficient of Determination (R^2): " + results[0]);
                System.out.println("Final Root Mean Square Error (RMSE): " + results[1]);
            }*/
		}
	}
}
