package org.commoncrawl.util.shared;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.util.Set;

/**
 * a basic SimHash implementation
 * 
 * @author rana
 *
 */
public class SimHash {
	public static final int  HASH_SIZE = 64;
	public static final long HASH_RANGE = 2^HASH_SIZE;
	public static MurmurHash hasher = new MurmurHash();
	
	/**
	 * use short cuts to obtains a speed optimized simhash calculation 
	 * 
	 * @param s input string 
	 * @return 64 bit simhash of input string
	 */
	
	private static final int FIXED_CGRAM_LENGTH = 4;
	
	public static long computeOptimizedSimHashForString(String s) { 
		return computeOptimizedSimHashForString(CharBuffer.wrap(s));
	}
	
	public static long computeOptimizedSimHashForString(CharBuffer s) { 
		
		LongSet shingles = new LongOpenHashSet(Math.min(s.length(),100000));
		
		int length = s.length();
		
		long timeStart = System.currentTimeMillis();
    for(int i=0; i<length-FIXED_CGRAM_LENGTH+1; i++) {
      // extract an ngram 
    	
    	long shingle = s.charAt(i);
    	shingle <<= 16;
    	shingle |= s.charAt(i + 1);
    	shingle <<= 16;
    	shingle |= s.charAt(i + 2);
    	shingle <<= 16;
    	shingle |= s.charAt(i + 3);
    	
    	shingles.add(shingle);
    }
    long timeEnd = System.currentTimeMillis();
    //System.out.println("NGram Production Took:" + (timeEnd-timeStart));
    
    
		int v[] = new int[HASH_SIZE];
		byte longAsBytes[] = new byte[8];
		
		for (long shingle : shingles) {
		
			longAsBytes[0] = (byte)(shingle >> 56);
			longAsBytes[1] = (byte)(shingle >> 48);
			longAsBytes[2] = (byte)(shingle >> 40);
			longAsBytes[3] = (byte)(shingle >> 32);
			longAsBytes[4] = (byte)(shingle >> 24);
			longAsBytes[5] = (byte)(shingle >> 16);
			longAsBytes[6] = (byte)(shingle >> 8);
			longAsBytes[7] = (byte)(shingle);
			
			long longHash = FPGenerator.std64.fp(longAsBytes, 0, 8);
			for (int i=0;i<HASH_SIZE;++i) { 
				boolean bitSet = ((longHash >> i) & 1L) == 1L;
				v[i] += (bitSet) ? 1 : -1;
			}
		}
		
		long simhash = 0;
		for (int i=0;i<HASH_SIZE;++i) { 
			if (v[i] > 0) { 
				simhash |= (1L << i);
			}
		}		
		return simhash;
	}

	
	
	public static long computeSimHashFromString(Set<String> shingles) { 
		
		int v[] = new int[HASH_SIZE];
		// compute a set of shingles
		for (String shingle : shingles) { 
			byte[] bytes = shingle.getBytes();
			long longHash = FPGenerator.std64.fp(bytes, 0, bytes.length);
			//long hash1 = hasher.hash(bytes, bytes.length, 0);
			//long hash2 = hasher.hash(bytes, bytes.length, (int)hash1);
			//long longHash = (hash1 << 32) | hash2;
			for (int i=0;i<HASH_SIZE;++i) { 
				boolean bitSet = ((longHash >> i) & 1L) == 1L;
				v[i] += (bitSet) ? 1 : -1;
			}
		}
		long simhash = 0;
		for (int i=0;i<HASH_SIZE;++i) { 
			if (v[i] > 0) { 
				simhash |= (1L << i);
			}
		}
		
		return simhash;
	}
	
	public static int hammingDistance(long hash1,long hash2) { 
		long bits = hash1 ^ hash2;
		int count = 0;
		while (bits != 0) { 
			bits &= bits - 1;
			++count;
		}
		return count;
	}
	
	public static long rotate(long hashValue) {
		return (hashValue << 1) | (hashValue >>> -1);
	}

	
	public static void main(String[] args) {
	  try { 
		  //File file1 = new File("/Users/rana/academia.edu_01.html");
		  //File file2 = new File("/Users/rana/academia.edu_02.html");
	  	
		  File file1 = new File(args[0]);
		  File file2 = new File(args[1]);
	  	
		  byte data1[] = new byte[(int) file1.length()];
		  byte data2[] = new byte[(int) file2.length()];
		  FileInputStream stream1 = new FileInputStream(file1);
		  FileInputStream stream2 = new FileInputStream(file2);
		  stream1.read(data1);
		  stream2.read(data2);
		  String string1 = new String(data1);
		  String string2 = new String(data2);

		  long timeStart = System.currentTimeMillis();
		  long simhash1 = computeSimHashFromString(Shingle.shingles(string1));
		  long timeEnd = System.currentTimeMillis();
		  System.out.println("Old Calc for Document A Took:" + (timeEnd - timeStart));
		  timeStart = System.currentTimeMillis();
		  long simhash2 = computeSimHashFromString(Shingle.shingles(string2));
		  timeEnd = System.currentTimeMillis();
		  System.out.println("Old Calc for Document B Took:" + (timeEnd - timeStart));
		  timeStart = System.currentTimeMillis();
		  long simhash3 = computeOptimizedSimHashForString(string1);
		  timeEnd = System.currentTimeMillis();
		  System.out.println("New Calc for Document A Took:" + (timeEnd - timeStart));
		  timeStart = System.currentTimeMillis();
		  long simhash4 = computeOptimizedSimHashForString(string2);
		  timeEnd = System.currentTimeMillis();
		  System.out.println("New Calc for Document B Took:" + (timeEnd - timeStart));
		  
		  int  hammingDistance = hammingDistance(simhash1, simhash2);
		  int  hammingDistance2 = hammingDistance(simhash3, simhash4);
		  
		  System.out.println("hammingdistance Doc (A) to Doc(B) OldWay:" + hammingDistance);
		  System.out.println("hammingdistance Doc (A) to Doc(B) NewWay:" + hammingDistance2);
	  }
	  catch (IOException e) { 
	  	e.printStackTrace();
	  }
	  
  }
	
}
