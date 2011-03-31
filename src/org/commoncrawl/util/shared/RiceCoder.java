package org.commoncrawl.util.shared;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

/**
 * Encode a set of Longs using Rice coding, a variant of Golomb coding,
 * a lossless compression scheme.   
 * 
 * @author rana
 *
 */
public class RiceCoder {

  private int size; // number of items stored

  private int nbits;  // number of bits to store the items

  private int m;    // parameter to this encoding

  private byte[] bits;  // the actual bits
  
  private boolean usesSignedEncoding = false;

  /**
   * 
   * @param mval the number of bits dedicated to binary encoding 
   */
  public RiceCoder(int mval,boolean usesSignedEncoding) {

    bits = new byte[4];
    m = mval;
    if (m < 0 || m > 64)
      throw new RuntimeException("m < 0 || m > 64");
    this.usesSignedEncoding = usesSignedEncoding;
  }
  
  /* construct a rice coding object from previously encoded data 
   * 
   */
  public RiceCoder(int mval,int size,int bitCount,byte[] bits,boolean usesSignedEncoding) { 
    this.m = mval;
    this.size = size;
    this.nbits = bitCount;
    this.bits = bits;
    this.usesSignedEncoding = usesSignedEncoding;
  }  
  
  // get the number of items
  public int getSize() {
    return size;
  }

  // get the number of bits
  public int getNumBits() {
    return nbits;
  }
  
  // get the number of bits as bytes 
  public int getNumBitsAsBytes() { 
    return (nbits + 7) / 8;
  }
  
  public int getMValue() { 
    return m;
  }
  
  /** get the underlying storage bits
   *  NOTE: array may have been overallocated. use getNumBits to idenitfy number of valid elements 
   * @return byte array used to store underlying bits  
   */
  public byte[] getBits() { 
    return bits;
  }

  // add an item to the encoding
  public int addItem(long val) {
    long x = val;
    int extra = 0;
    int bitsUsed = 0;
    if (usesSignedEncoding) { 
      if (val < 0) { 
        addbit(1);
        bitsUsed++;
        if (val != Long.MIN_VALUE) {  
          x = -val;
        }
      }
      else { 
        addbit(0);
        bitsUsed++;
      }
    }
    size++;

    long q = Math.abs(x >> m);
    
    bitsUsed += q;
    bitsUsed += m;

    long r = x & ((1L << m) - 1L);

    // encode the first (unary) part
    while (q-- > 0)
      addbit(1);
    if (extra != 0) 
      addbit(1);
    addbit(0);

    // encode the binary part
    if (m > 0) {

      long mask = (1L << (m - 1));

      while (mask != 0) {
        addbit((r & mask) != 0 ? 1 : 0);

        mask >>= 1;
      }
    }
    return bitsUsed;
  }
  
  
  // get the value of the n-th bit
  private final int getbit(int n) {
    return (bits[(n >> 3)] >> (n & 0x7)) & 0x1;
  }
  
  // add a bit to the encoding
  private final void addbit(int b) {

    int len = bits.length;
    // need to grow the bit list
    if (nbits == len * 8) {
      int newlen = (int)(len * 1.5) + 1;
      byte tmp[] = new byte[newlen];
      System.arraycopy(bits, 0, tmp, 0, bits.length);
      bits = tmp;
    }
    if (b == 1)
      bits[(nbits >> 3)] |= (1 << (nbits & 0x7));
    nbits++;
  }


  /**
   * 
   * @return the items in this encoding as a vector
   */
  public long[] getItems() {

    long items[] = new long[size];

    int currbit = 0;

    for (int i = 0; i < size; i++) {

      int  isNegative = 0;
      long unary = 0;

      if (usesSignedEncoding) { 
        isNegative = getbit(currbit++);
      }
      while (getbit(currbit) != 0) {
        unary++;
        currbit++;
      }

      currbit++;
      long binary = 0;
      for (int j = 1; j <= m; j++)
        binary = (binary << 1) | getbit(currbit++);
      if (isNegative == 0)
        items[i] = (unary << m) + binary;
      else 
        items[i] = -(unary << m) -binary;
    }
    return items;
  }
  
  public static final class RiceCodeReader { 
    
    private int m;
    private int currbit = 0;
    private int nbits;
    private ByteBuffer bits;
    private boolean usesSignedEncoding = false;
    
    public RiceCodeReader(int mValue,int totalBits,ByteBuffer array,boolean usesSignedEncoding) { 
      this.m = mValue;
      this.nbits = totalBits;
      this.bits = array;
      this.currbit = 0;
      this.usesSignedEncoding = usesSignedEncoding;
    }
    
    public boolean hasNext() { 
      return currbit < nbits;
    }

    public int getbit() { 
      return getbit(currbit++);      
    }
    
    // get the value of the n-th bit
    private int getbit(int bitNo) {
      return (bits.get((bitNo >> 3)) >> (bitNo & 0x7)) & 0x1;        
    }
    
    public long nextValue() { 
      int  isNegative = 0;
      long unary = 0;

      if (usesSignedEncoding) { 
        isNegative = getbit(currbit++);
      }
      
      int currByte    = bits.get((currbit >> 3));
      
      while (((currByte >> (currbit & 0x7)) & 0x1) != 0) {
        unary++;
        if ((currbit & 0x7) == 0x7) { 
          currByte  = bits.get(((currbit + 1) >> 3));
        }
        currbit++;
      }

      currbit++;

      currByte = bits.get(currbit >> 3);
      
      long binary = 0;
      for (int j = 1; j <= m;) {
        binary = (binary << 1) | ((currByte >> (currbit & 0x7)) & 0x1);
        
        j++;
        if (j <= m) {
          if ((currbit & 0x7) == 0x7) { 
            currByte    = bits.get(((currbit + 1) >> 3));
          }
        }
        currbit++;
      }
      
      if (isNegative == 1) {
        return -(unary << m) - binary;
      }
      else { 
        return (unary << m) + binary;
      }
    }
  }
  
  private static double lg(double value) { 
    return Math.log(value)/Math.log(2.0);
  }   
  
  public static void main(String[] args) {
    long foo = Long.MIN_VALUE;
    RiceCoder test = new RiceCoder(54,true);
    test.addItem(0);
    test.addItem(1);
    test.addItem(-1);
    test.addItem(Long.MAX_VALUE);
    test.addItem(Long.MAX_VALUE - 1);
    test.addItem(Long.MIN_VALUE + 1);
    test.addItem(Long.MIN_VALUE);
    RiceCodeReader testReader = new RiceCodeReader(54,test.nbits,ByteBuffer.wrap(test.bits),true); 
    System.out.println(testReader.nextValue());
    System.out.println(testReader.nextValue());
    System.out.println(testReader.nextValue());
    System.out.println(testReader.nextValue());
    System.out.println(testReader.nextValue());
    System.out.println(testReader.nextValue());
    System.out.println(testReader.nextValue());
    
    long fps[] = new long[1000];
    for (int i=0;i<1000;++i) { 
      fps[i] = FPGenerator.std64.fp("test" + i);
    }
    Arrays.sort(fps);
    
    // compute m
    double cumilative = 0.0;
    for (int i=0;i<fps.length;++i) { 
      cumilative += fps[i];
    }
    double avgDelta = cumilative / (double)fps.length;
    System.out.println("Delta is:" + avgDelta);
    int mForFingerprints =  (int) Math.floor(lg(avgDelta));
    System.out.println("Delta is:" + mForFingerprints);
    RiceCoder coding = new RiceCoder(mForFingerprints,false);
    long lastValue=fps[0];
    DataOutputBuffer testBuffer = new DataOutputBuffer();
    for (int i=1;i<fps.length;++i) {
      long delta =  fps[i] - lastValue;
      coding.addItem(delta + 1);
      try {
        WritableUtils.writeVLong(testBuffer, delta);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      lastValue = fps[i];
    }
    System.out.println("Number of Bits:" + coding.nbits);
    System.out.println("Number of Bytes:" + (coding.nbits >> 3));
    System.out.println("Number of VLong Bytes:" + testBuffer.getLength());
    System.out.println("FP Bytes:" + ((fps.length -1) * 8));

    double fileIdTotal = 0.0;
    int fileId[] = new int[1000];
    int fileIdLast = 0;
    for (int i=0;i<fileId.length;++i) { 
      fileId[i] = (int) (Math.random() * 32000); 
      fileIdTotal += Math.abs((fileId[i] - fileIdLast));
      fileIdLast = fileId[i];
    }
    
    try { 
      DataOutputBuffer gzipOutput = new DataOutputBuffer();
      GZIPOutputStream gzipStream = new GZIPOutputStream(gzipOutput);
      DataOutputStream gzipData   = new DataOutputStream(gzipStream);
      
      double fileIdAvg = fileIdTotal / fileId.length;
      System.out.println("FileId Avg Value:" + fileIdAvg);
      int mForFileIds =  (int) Math.floor(lg(fileIdAvg));
      RiceCoder fileIdCoding = new RiceCoder(mForFileIds,true);
      DataOutputBuffer testBuffer2 = new DataOutputBuffer();

      int fieldIdLast = 0;
      for (int i=0;i<fileId.length;++i) { 
        fileIdCoding.addItem((fileId[i] - fieldIdLast) + 1);
        System.out.println("FieldId Delta:" + (fileId[i] - fieldIdLast));
        fieldIdLast = fileId[i];
        gzipData.writeInt(fileId[i]);
        WritableUtils.writeVInt(testBuffer2,fileId[i]);
      }
      gzipData.flush();
      gzipStream.flush();
      gzipStream.close();
      
      System.out.println("FileId Compressed Bytes:" + (fileIdCoding.nbits >> 3));
      System.out.println("FileId Bytes:" + (fileId.length * 4));
      System.out.println("FileId GZIPBytes :" + gzipOutput.getLength());
      System.out.println("FileId VLong Bytes:" + testBuffer2.getLength());
      
      for (int i=0;i<100;++i) { 
        RiceCodeReader reader = new RiceCodeReader(mForFingerprints,coding.nbits,ByteBuffer.wrap(coding.bits),false);
        long startTime = System.currentTimeMillis();
        while (reader.hasNext()) { 
          reader.nextValue();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Time Elapshed:" + (endTime - startTime));
      }
    }
    catch (IOException e) { 
      e.printStackTrace();
    }
  }
  
}

