package org.apache.flink.transformations;

import com.mellowtech.collections.BPlusTree;
import com.mellowtech.collections.KeyValue;
import com.mellowtech.disc.CBString;

import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.tools.*;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.*;

/**
 *
 */
public class ReducePhase2
		extends RichGroupReduceFunction<CurveRecord, Tuple4<String, String, String, String>> {

	
	/**
	* Environment specific temporary directory.
	*/
	private static final String PROPERTY_TMP_DIRECTORY = "java.io.tmpdir";

	/**
	* Creates a directory.
	*
	* @param dir the directory to create.
	* @throws IOException if an I/O exception occurs.
	*/
	private static void ensureDirectory(File dir) throws IOException {
	    if (!dir.mkdirs() && !dir.isDirectory()) {
	        throw new IOException("Mkdirs failed to create " + dir.toString());
	    }
	}
	
	private static final long serialVersionUID = 1L;
	private int zOffset, ridOffset, srcOffset, sidOffset, classOrValueOffset;
	private ExecConf conf;

	/**
	 *
	 * @param conf
	 */
	public ReducePhase2(ExecConf conf) {
		this.conf = conf;
	}

	/**
	 *
	 * @param input
	 * @param output
	 * @throws Exception 
	 */
	@Override
	public void reduce(Iterable<CurveRecord> input,
					   Collector<Tuple4<String, String, String, String>> output) throws Exception {

		Iterator<CurveRecord> iterator = input.iterator();
		CurveRecord entry = iterator.next();
		
		File tmpDir = new File(System.getProperty(PROPERTY_TMP_DIRECTORY));
		ensureDirectory(tmpDir);

		String outerTable = "R_local" + entry.getFourth();
		String innerTable = "S_local" + entry.getFourth();
		
		final File fileOuter;
		final File fileInner;
		
		try {
			fileOuter = File.createTempFile(outerTable, "", tmpDir);
			fileInner = File.createTempFile(innerTable, "", tmpDir);
		} catch (IOException ioe) {
		    throw new Exception(String.format("Error creating temp dir in java.io.tmpdir %s due to %s.", tmpDir, ioe.getMessage()));
		}
		
		//File R_local = new File("R_local" + entry.getFourth() + "/");
		//R_local.mkdirs();
		
		//File S_local = new File("S_local" + entry.getFourth() + "/");
		//S_local.mkdirs();

		//String outerTable = "R_local" + entry.getFourth() + "/R_local";
		//String innerTable = "S_local" + entry.getFourth() + "/S_local";

		//File fileOuter = new File(outerTable);
		//File fileInner = new File(innerTable);

		zOffset = 0;
		ridOffset = zOffset + 1;
		srcOffset = ridOffset + 1;
		sidOffset = srcOffset + 1;
		classOrValueOffset = sidOffset + 1;

		String line = entry.toString();
		String[] parts = line.split(" +");
		// if (Integer.parseInt(parts[sidOffset]) >= conf.getNumOfPartition()) sid = 1;

		// Create seperate local files for different key value
		int bufferSize = 8 * 1024 * 1024;
		FileWriter fwForR = new FileWriter(fileOuter);
		BufferedWriter writerR = new BufferedWriter(fwForR, bufferSize);
		FileWriter fwForS = new FileWriter(fileInner);
		BufferedWriter writerS = new BufferedWriter(fwForS, bufferSize);

		while (iterator.hasNext()) {

			line = entry.toString();

			parts = line.split(" +");
			String zvalue = parts[zOffset];
			String rid = parts[ridOffset];
			String src = parts[srcOffset];
			int srcId = Integer.valueOf(src);
			String theClass = parts[classOrValueOffset];

			String tmpRecord = zvalue + " " + rid + ":" + theClass + "\n";

			if (srcId == 0) // from R
				writerR.write(tmpRecord);
			else if (srcId == 1) // from S
				writerS.write(tmpRecord);
			else {
				System.out.println(srcId);
				System.out.println("The record has an unknown source!!");
				System.exit(-1);
			}
			entry = iterator.next();
		}

		writerR.close();
		writerS.close();

		/****** Perform binary search for each R over the S ********/
		/** source: http://www.cs.utah.edu/~lifeifei/knnj/#codes **/
		CBString keyType = new CBString();
		CBString valueType = new CBString();
		int indexBlockSize = 1024 * 32; // 4k size
		int valueBlockSize = 1024 * 32;
		int bufInLength = 8 * 1024 * 1024;

		BPlusTree bpt = new BPlusTree(fileInner.getPath(), keyType, valueType,
				valueBlockSize, indexBlockSize);
		bpt.setTreeCache(32 * 1024 * 1024, 32 * 1024 * 1024);

		int flag = 0;
		bpt.createIndexBL(fileInner.getPath(), bufInLength, flag);
		bpt.save();
		float hashTableLoadFactor = 0.75f;
		final int knnFactor = 4;

		int hashTableCapacity = (int) Math.ceil((knnFactor * conf.getKnn())
				/ hashTableLoadFactor) + 1;

		LinkedHashMap<String, ArrayList<Integer>> coordLRUCache =
				new LinkedHashMap<String, ArrayList<Integer>>(
						hashTableCapacity, hashTableLoadFactor, true) {
					private static final long serialVersionUID = 1L;

					@Override
					protected boolean removeEldestEntry(
							Map.Entry<String, ArrayList<Integer>> eldest) {
						return size() > knnFactor * conf.getKnn();
					}
				};

		FileReader frForR = new FileReader(fileOuter.getPath());
		BufferedReader brForR = new BufferedReader(frForR, bufInLength);

		boolean loop = true;
		while (loop) {
			line = brForR.readLine();
			if (line == null)
				break;

			parts = line.split(" +");
			String val = parts[0];
			String rid = parts[1];

			int[] coord = null;
			if (conf.getHOrZOrG() == 1) coord = Horder.toCoord(val, conf);
			else if (conf.getHOrZOrG() == 2) coord = Zorder.toCoord(val, conf.getDimension());
			else if (conf.getHOrZOrG() == 3) coord = Gorder.toCoord(val, conf.getDimension());
			else System.out.println("Error! Wrong curve code!");

			// Unshift (not necessary)
			/*
			if (sid != 0) {
				for (int i=0; i<conf.getDimension(); i++)
					coord[i] = coord[i] - conf.getShiftVectors()[sid][i];
			}*/

			ArrayList<ArrayList<KeyValue>> knnList = bpt.rangeSearch(
					new CBString(val), conf.getKnn());

			ArrayList<KnnRecord> knnListSorted = new ArrayList<KnnRecord>();
			Comparator<KnnRecord> krc = new KnnRecordComparator();
			for (ArrayList<KeyValue> l : knnList) {
				for (KeyValue e : l) {

					String val2 = ((CBString) e.getKey()).getString();
					String rid2 = ((CBString) e.getValue()).getString();
					int[] coord2 = null;

					ArrayList<Integer> cachedCoord2 = coordLRUCache.get(val2);

					if (cachedCoord2 == null) {
						if (conf.getHOrZOrG() == 1) coord2 = Horder.toCoord(val2, conf);
						else if (conf.getHOrZOrG() == 2) coord2 = Zorder.toCoord(val2, conf.getDimension());
						else if (conf.getHOrZOrG() == 3) coord2 = Gorder.toCoord(val2, conf.getDimension());
						else System.out.println("Error! Wrong curve code!");

						// Unshift (not necessary)
						/*
						if (sid != 0) {
							for (int i=0; i<conf.getDimension(); i++)
								coord2[i] = coord2[i] - conf.getShiftVectors()[sid][i];
						}
						*/

						ArrayList<Integer> ai = new ArrayList<Integer>(
								conf.getDimension());
						for (int i = 0; i < conf.getDimension(); i++) {
							ai.add(coord2[i]);
						}
						coordLRUCache.put(val2, ai);
					} else {
						coord2 = new int[conf.getDimension()];
						for (int i = 0; i < conf.getDimension(); i++)
							coord2[i] = cachedCoord2.get(i);
					}

					float dist = (float) 0.0;
					for (int i = 0; i < conf.getDimension(); i++)
						dist += (float) Math.abs(((coord[i] - coord2[i]) * (coord[i] - coord2[i])));

					KnnRecord kr = new KnnRecord(rid2, (float) Math.sqrt(dist), coord2);
					knnListSorted.add(kr);
				}
			}

			Collections.sort(knnListSorted, krc);

			KnnRecord tmp = knnListSorted.get(0);
			for (int i = 0; i < conf.getKnn(); i++) {
				KnnRecord kr;
				try {
					kr = knnListSorted.get(i);
				} catch (IndexOutOfBoundsException e) {
					kr = tmp;
				}

				int k = kr.getRid().indexOf(":");
				output.collect(new Tuple4<>(rid, kr.getRid().substring(0, k), Float.toString(kr.getDist()),
						kr.getRid().substring(k + 1)));
			}
		}
		brForR.close();
	}

	/**
	 * source: http://www.cs.utah.edu/~lifeifei/knnj/#codes
	 **/
	class KnnRecordComparator implements Comparator<KnnRecord> {

		/**
		 *
		 * @param o1
		 * @param o2
		 * @return
		 */
		public int compare(KnnRecord o1, KnnRecord o2) {
			int ret = 0;
			float dist = o1.getDist() - o2.getDist();

			if (dist > 0) ret = 1;
			else if (dist < 0) ret = -1;
			return -ret;
		}
	}
}
