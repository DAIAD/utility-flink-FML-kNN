package org.apache.flink.tools;

import java.math.BigInteger;

/**
 *
 */
public class Horder {

	private static int NUMBITS = 32;
	private static int WORDBITS = 32;

	/**
	 * Function that encodes a point in Hilbert mapping
	 * source: http://www.dcs.bbk.ac.uk/~jkl/publications.html
	 * @param coord
	 * @return Point
	 */
	static long[] HilbertEncode(int[] coord, ExecConf conf) {

		long mask = (long) 1 << WORDBITS - 1, element, temp1, temp2, A, W = 0, S, tS, T, tT, J, P = 0, xJ;
		long[] h = new long[conf.getDimension()];
		int i = NUMBITS * conf.getDimension() - conf.getDimension(), j;

		for (j = (int) (A = 0); j < conf.getDimension(); j++)
			if ((coord[j] & mask) != 0)
				A |= (1 << conf.getDimension() - 1 - j);

		S = tS = A;

		P |= S & (1 << conf.getDimension() - 1 - 0);
		for (j = 1; j < conf.getDimension(); j++)
			if ((S & (1 << conf.getDimension() - 1 - j) ^ (P >> 1) & (1 << conf.getDimension() - 1 - j)) != 0)
				P |= (1 << conf.getDimension() - 1 - j);

	    /* add in conf.getDimension() bits to hcode */
		element = i / WORDBITS;
		if (i % WORDBITS > WORDBITS - conf.getDimension()) {
			h[(int) element] |= P << i % WORDBITS;
			h[(int) (element + 1)] |= P >> WORDBITS - i % WORDBITS;
		} else
			h[(int) element] |= P << i - element * WORDBITS;

		J = conf.getDimension();
		for (j = 1; j < conf.getDimension(); j++)
			if ((P >> j & 1) == (P & 1))
				continue;
			else
				break;
		if (j != conf.getDimension())
			J -= j;
		xJ = J - 1;

		if (P < 3)
			T = 0;
		else if ((P % 2) != 0)
			T = (P - 1) ^ (P - 1) / 2;
		else
			T = (P - 2) ^ (P - 2) / 2;
		tT = T;

		for (i -= conf.getDimension(), mask >>= 1; i >= 0; i -= conf.getDimension(), mask >>= 1) {
			for (j = (int) (A = 0); j < conf.getDimension(); j++)
				if ((coord[j] & mask) != 0)
					A |= (1 << conf.getDimension() - 1 - j);

			W ^= tT;
			tS = A ^ W;
			if ((xJ % conf.getDimension()) != 0) {
				temp1 = tS << xJ % conf.getDimension();
				temp2 = tS >> conf.getDimension() - xJ % conf.getDimension();
				S = temp1 | temp2;
				S &= ((long) 1 << conf.getDimension()) - 1;
			} else
				S = tS;

			P = S & (1 << conf.getDimension() - 1 - 0);
			for (j = 1; j < conf.getDimension(); j++)
				if ((S & (1 << conf.getDimension() - 1 - j) ^ (P >> 1) & (1 << conf.getDimension() - 1 - j)) != 0)
					P |= (1 << conf.getDimension() - 1 - j);

	        /* add in conf.getDimension() bits to hcode */
			element = i / WORDBITS;
			if ((i % WORDBITS) > (WORDBITS - conf.getDimension())) {
				h[(int) element] |= P << (i % WORDBITS);
				h[(int) (element + 1)] |= P >> ((WORDBITS - i) % WORDBITS);
			} else
				h[(int) element] |= P << i - element * WORDBITS;

			if (i > 0) {
				if (P < 3)
					T = 0;
				else if ((P % 2) != 0)
					T = (P - 1) ^ (P - 1) / 2;
				else
					T = (P - 2) ^ (P - 2) / 2;

				if (xJ % conf.getDimension() != 0) {
					temp1 = T >> xJ % conf.getDimension();
					temp2 = T << conf.getDimension() - xJ % conf.getDimension();
					tT = temp1 | temp2;
					tT &= ((long) 1 << conf.getDimension()) - 1;
				} else
					tT = T;

				J = conf.getDimension();
				for (j = 1; j < conf.getDimension(); j++)
					if ((P >> j & 1) == (P & 1))
						continue;
					else
						break;
				if (j != conf.getDimension())
					J -= j;

				xJ += J - 1;
			/*	J %= conf.getDimension();*/
			}
		}
		return h;
	}

	/**
	 * Function that decodes a Hilbert encoded point to the initial point
	 * source: http://www.dcs.bbk.ac.uk/~jkl/publications.html
	 * @param encodedCoords
	 * @return Point
	 */
	static long[] HilbertDecode(long[] encodedCoords, ExecConf conf) {
		long mask = (long) 1 << WORDBITS - 1, element, temp1, temp2, A, W = 0, S, tS, T, tT, J, P = 0, xJ;
		long[] pt = new long[conf.getDimension()];
		int i = NUMBITS * conf.getDimension() - conf.getDimension(), j;


	    /*--- P ---*/
		element = i / WORDBITS;
		P = encodedCoords[(int) element];
		if (i % WORDBITS > WORDBITS - conf.getDimension()) {
			temp1 = encodedCoords[(int) (element + 1)];
			P >>= i % WORDBITS;
			temp1 <<= WORDBITS - i % WORDBITS;
			P |= temp1;
		} else
			P >>= i % WORDBITS;	/* P is a conf.getDimension() bit hcode */

	    /* the & masks out spurious highbit values */
		if (conf.getDimension() < WORDBITS)
			P &= (1 << conf.getDimension()) - 1;

	    /*--- xJ ---*/
		J = conf.getDimension();
		for (j = 1; j < conf.getDimension(); j++)
			if ((P >> j & 1) == (P & 1))
				continue;
			else
				break;
		if (j != conf.getDimension())
			J -= j;
		xJ = J - 1;

	    /*--- S, tS, A ---*/
		A = S = tS = P ^ P / 2;


	    /*--- T ---*/
		if (P < 3)
			T = 0;
		else if ((P % 2) != 0)
			T = (P - 1) ^ (P - 1) / 2;
		else
			T = (P - 2) ^ (P - 2) / 2;

	    /*--- tT ---*/
		tT = T;

	    /*--- distrib bits to coords ---*/
		for (j = conf.getDimension() - 1; P > 0; P >>= 1, j--)
			if ((P & 1) != 0)
				pt[j] |= mask;


		for (i -= conf.getDimension(), mask >>= 1; i >= 0; i -= conf.getDimension(), mask >>= 1) {
	        /*--- P ---*/
			element = i / WORDBITS;
			P = encodedCoords[(int) element];
			if (i % WORDBITS > WORDBITS - conf.getDimension()) {
				temp1 = encodedCoords[(int) (element + 1)];
				P >>= i % WORDBITS;
				temp1 <<= WORDBITS - i % WORDBITS;
				P |= temp1;
			} else
				P >>= i % WORDBITS;	/* P is a conf.getDimension() bit hcode */

	        /* the & masks out spurious highbit values */
			if (conf.getDimension() < WORDBITS)
				P &= (1 << conf.getDimension()) - 1;

	        /*--- S ---*/
			S = P ^ P / 2;

	        /*--- tS ---*/
			if (xJ % conf.getDimension() != 0) {
				temp1 = S >> xJ % conf.getDimension();
				temp2 = S << conf.getDimension() - xJ % conf.getDimension();
				tS = temp1 | temp2;
				tS &= ((long) 1 << conf.getDimension()) - 1;
			} else
				tS = S;

	        /*--- W ---*/
			W ^= tT;

	        /*--- A ---*/
			A = W ^ tS;

	        /*--- distrib bits to coords ---*/
			for (j = conf.getDimension() - 1; A > 0; A >>= 1, j--)
				if ((A & 1) != 0)
					pt[j] |= mask;

			if (i > 0) {
	            /*--- T ---*/
				if (P < 3)
					T = 0;
				else if ((P % 2) != 0)
					T = (P - 1) ^ (P - 1) / 2;
				else
					T = (P - 2) ^ (P - 2) / 2;

	            /*--- tT ---*/
				if (xJ % conf.getDimension() != 0) {
					temp1 = T >> xJ % conf.getDimension();
					temp2 = T << conf.getDimension() - xJ % conf.getDimension();
					tT = temp1 | temp2;
					tT &= ((long) 1 << conf.getDimension()) - 1;
				} else
					tT = T;

	            /*--- xJ ---*/
				J = conf.getDimension();
				for (j = 1; j < conf.getDimension(); j++)
					if ((P >> j & 1) == (P & 1))
						continue;
					else
						break;
				if (j != conf.getDimension())
					J -= j;
				xJ += J - 1;
			}
		}
		return pt;
	}

	/**
	 *
	 * @param coord
	 * @param conf
	 * @return
	 */
	public static String valueOf(int[] coord, ExecConf conf) {

		int fix = Functions.maxDecDigits(conf.getDimension());
		long[] encodedPoints = HilbertEncode(coord, conf);
		String result = "";
		String extra = "";

		for (int i = encodedPoints.length - 1; i >= 0; i--) {
			extra = Functions.createExtra((WORDBITS) - Long.toBinaryString(encodedPoints[i]).length());
			String binary = extra + Long.toBinaryString(encodedPoints[i]);
			result = result + binary;
		}

		String order = new String(result);
		BigInteger ret = new BigInteger(order, 2);
		order = ret.toString();
		if (order.length() < fix) {
			extra = Functions.createExtra(fix - order.length());
			order = extra + order;
		} else if (order.length() > fix) {
			System.out.println("too big hilbert code, need to fix HilbertOrder.java");
			System.exit(-1);
		}

		//int[] tmp = toCoord(order, conf);
		return order;
	}

	/**
	 *
	 * @param hilbert
	 * @param conf
	 * @return
	 */
	public static int[] toCoord(String hilbert, ExecConf conf) {

		int DECIMAL_RADIX = 10;
		int BINARY_RADIX = 2;

		if (hilbert == null) {
			System.out.println("Hilbert order null pointer!!!@HilbertOrder.toCoord");
			System.exit(-1);
		}

		BigInteger bigH = new BigInteger(hilbert, DECIMAL_RADIX);
		String bigHStr = bigH.toString(BINARY_RADIX);

		String extra = Functions.createExtra(conf.getDimension() * (WORDBITS) - bigHStr.length());
		bigHStr = extra + bigHStr;

		long[] encodedCoords = new long[conf.getDimension()];

		int tmp = 0;
		for (int i = conf.getDimension() - 1; i >= 0; i--) {
			String element = bigHStr.substring(tmp * (WORDBITS), (tmp + 1) * (WORDBITS));
			Long tmpLong = Long.parseLong(element, 2);
			encodedCoords[i] = tmpLong;
			tmp++;
		}

		long[] decodedPoints = HilbertDecode(encodedCoords, conf);
		int[] decodedIntPts = new int[conf.getDimension()];

		tmp = 0;
		for (long point : decodedPoints) {
			decodedIntPts[tmp] = (int) point;
			tmp++;
		}

		return decodedIntPts;
	}
}