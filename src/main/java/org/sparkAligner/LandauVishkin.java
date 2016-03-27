package org.sparkAligner;

import java.io.IOException;


///                  k*2+1   (k+1)^2
///   k=0      1       1        1 
///   k=1     123      3        4
///   k=2    12345     5        9
///   k=3   1234567    7        16
///   k=4  123456789   9        25



public final class LandauVishkin {

	private static AlignInfo noAlignment   = new AlignInfo(0,  0, null, null, 0);
	private static AlignInfo badAlignment  = new AlignInfo(-1,-1, null, null, 0);
	private static AlignInfo goodAlignment = new AlignInfo(0, 0, null, null, 0);
	
	private static int [][] L = null;
	private static int [][] B = null;
	private static int [] dist = null;
	private static int [] what = null;	
	
	
	//------------------------- configure --------------------------
	// initialize runtime buffers
	
	public static void configure(int k)
	{
		L    = new int [k*2+1][k+1];
		B    = new int [k*2+1][k+1];
		dist = new int [k+1];
		what = new int [k+1];
	}
	
	
	//------------------------- kmismatch --------------------------
	// count mismatches between ascii strings
	
	public static AlignInfo kmismatch(byte [] text, byte [] pattern, int k)
	{		
		int m = pattern.length;
		int n = text.length;
		
		if (m == 0 || n == 0)
		{
			return noAlignment;
		}
		
		int last = (m < n) ? m : n;
		int mm = 0;
		int match = 0;
		
		for (int pos = 0; pos < last; pos++)
		{
			if (text[pos] != pattern[pos])
			{
				what[mm] = 0;
				dist[mm] = match;
				match = 0;
				
				mm++;
				
				if (mm > k)
				{
					return badAlignment;
				}
			}
			
			match++;
		}
		
		dist[mm] = match;
		what[mm] = 2;
				
		goodAlignment.setVals(last, mm, dist, what, mm+1);	// say how far we reached in the text (reference)
		return goodAlignment;
	}

	
	//------------------------- kmismatch_bin --------------------------
	// count mismatches between 2 bases / byte binary strings
	
	public static AlignInfo kmismatch_bin(byte [] text, byte [] pattern, int k)
	{
		int m = pattern.length;
		int n = text.length;
		
		if (m == 0)
		{
			return noAlignment;
		}
		
		// require the entire query to align
		if (n < m)
		{
			return badAlignment;
		}
		
		int last = (m < n) ? m : n;
		int mm = 0;
		int match = 0;
		
		last--; // check the last position outside of the loop so we can check for a space
		
		int pos = 0;
		for (; pos < last; pos++)
		{
			if (text[pos] == pattern[pos])
			{
				match += 2;
			}
			else
			{
				if ((text[pos] & 0xF0) != (pattern[pos] & 0xF0))
				{
					dist[mm] = match;
					match = 0;
					mm++;
					
					if (mm > k)
					{
						return badAlignment;
					}
				}
				
				match++;
				
				if ((text[pos] & 0x0F) != (pattern[pos] & 0x0F))
				{
					dist[mm] = match;
					match = 0;
					mm++;
					
					if (mm > k)
					{
						return badAlignment;
					}
				}
				
				match++;
			}
		}
			
		int alignlen = last*2+1;

		// explicitly check the last 2 characters since last 1 may be a space
		if (((text[pos] & 0x0F) != DNAString.space) && ((pattern[pos] & 0x0F) != DNAString.space))
		{
			alignlen++;
			
			if (text[pos] == pattern[pos])
			{
				match += 2;
			}
			else
			{
				if ((text[pos] & 0xF0) != (pattern[pos] & 0xF0))
				{
					dist[mm] = match;
					match = 0;
					mm++;

					if (mm > k)
					{
						return badAlignment;
					}
				}

				match++;

				if ((text[pos] & 0x0F) != (pattern[pos] & 0x0F))
				{
					dist[mm] = match;
					match = 0;
					mm++;

					if (mm > k)
					{
						return badAlignment;
					}
				}

				match++;
			}
		}
		else
		{
			if ((text[pos] & 0xF0) != (pattern[pos] & 0xF0))
			{
				dist[mm] = match;
				match = 0;
				mm++;

				if (mm > k)
				{
					return badAlignment;
				}
			}

			match++;			
		}
		
		
		// only fill in 'what' if there are <= k mismatches
		for (int i = 0; i < mm; i++)
			what[i] = 0;
		
		dist[mm] = match;
		what[mm] = 2;
				
		goodAlignment.setVals(alignlen, mm, dist, what, mm+1);	// say how far we reached in the text (reference)
		return goodAlignment;
	}

	
	
	//------------------------- kdifference --------------------------
	// Landau-Vishkin k-difference algorithm to align strings
	
	public static AlignInfo kdifference(byte [] text, byte [] pattern, int k)
	{	
		int m = pattern.length;
		int n = text.length;
		
		if (m == 0 || n == 0)
		{
			return noAlignment;
		}
			
		// Compute the dynamic programming to see how the strings align
		for (int e = 0; e <= k; e++)
		{
			for (int d = -e; d <= e; d++)
			{
				int row = -1;
				
				if (e > 0)
				{
					if (java.lang.Math.abs(d) < e)
					{
						int up = L[k+d][e-1] + 1;
						if (up > row) { row = up; B[k+d][e] = 0; }
					}
					
					if (d > -(e-1))
					{
						int left = L[k+d-1][e-1];
						if (left > row) { row = left; B[k+d][e] = -1; }
					}
					
					if (d < e-1)
					{
						int right = L[k+d+1][e-1]+1;
						if (right > row) { row = right; B[k+d][e] = +1; }
					}
				}
				else
				{
					row = 0;
				}
				
				while ((row < m) && (row+d < n) && (pattern[row] == text[row+d]))
				{
					row++;
				}
				
				L[k+d][e] = row;
				
				//System.out.println("L: k:" + k + " d:" + d + " e:" + e + " = " + row);
				
				if ((row+d == n) || (row == m)) // reached the end of the pattern or text
				{		
					int distlen = e+1;
					
					int E = e;
					int D = d;
					
					what[E] = 2; // always end at end-of-string
					
					while (e >= 0)
					{
						int b = B[k+d][e];
						if (e > 0) { what[e-1] = b; }
						
						dist[e] = L[k+d][e];	
						if (e < E) { dist[e+1] -= dist[e]; }
						
						d += b;
						e--;	
					}
					
					goodAlignment.setVals(row+D, E, dist, what, distlen);	// say how far we reached in the text (reference)			
					return goodAlignment;				
				}
			}
		}
		
		return badAlignment;
	}
	
	
	//------------------------- extend --------------------------
	// align the strings either for either k-mismatch or k-difference
	
	public static AlignInfo extend(byte [] refbin, byte [] qrybin, int K, boolean ALLOW_DIFFERENCES) throws IOException
	{
		if (ALLOW_DIFFERENCES)
		{
			byte [] ref = DNAString.dnaToArr(refbin);
			byte [] qry = DNAString.dnaToArr(refbin);
			
			return kdifference(ref, qry, K);						
		}
		else
		{
			return kmismatch_bin(refbin, qrybin, K);
		}
	}
	
}

