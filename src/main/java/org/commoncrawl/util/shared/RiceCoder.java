package org.commoncrawl.util.shared;

/**
* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 **/


import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Encode a set of Longs using Rice coding, a variant of Golomb coding,
 * a lossless compression scheme.   
 * 
 * @author rana
 *
 */
public class RiceCoder {

  static final Log LOG = LogFactory.getLog(RiceCoder.class);  
  
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
    private FSDataInputStream stream; 
    private long dataOffset;
    private int  currByte;
    private int  currByteNo = -1;
    
    public RiceCodeReader(int mValue,int totalBits,ByteBuffer array,boolean usesSignedEncoding) { 
      this.m = mValue;
      this.nbits = totalBits;
      this.bits = array;
      this.currbit = 0;
      this.usesSignedEncoding = usesSignedEncoding;
    }

    public RiceCodeReader(int mValue,int totalBits,FSDataInputStream stream,long dataOffset,boolean usesSignedEncoding) { 
      this.m = mValue;
      this.nbits = totalBits;
      this.bits = null;
      this.currbit = 0;
      this.usesSignedEncoding = usesSignedEncoding;
      this.stream = stream;
      this.dataOffset = dataOffset;
     
    }
    
    public void close() { 
      if (stream != null) { 
        try {
          stream.close();
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
        stream = null;
      }
    }
    
    public boolean hasNext() { 
      return currbit < nbits;
    }

    public int getbit()throws IOException { 
      return getbit(currbit++);      
    }
    
    // get the value of the n-th bit
    private int getbit(int bitNo)throws IOException {
      if (stream == null) { 
        return (bits.get((bitNo >> 3)) >> (bitNo & 0x7)) & 0x1;
      }
      else { 
        int byteNo = bitNo >> 3;
        if (currByteNo != byteNo) {
          stream.seek(dataOffset + byteNo);
          currByte = stream.read();
          currByteNo = byteNo;
        }
        return (currByte >> (bitNo & 0x7)) & 0x1;
      }
    }
    
    public long nextValue() throws IOException { 
      int  isNegative = 0;
      long unary = 0;

      if (usesSignedEncoding) { 
        isNegative = getbit(currbit++);
      }
      while (getbit(currbit++) != 0) {      
        unary++;
      }
      
      long binary = 0;
      for (int j = 1; j <= m;) {
        binary = (binary << 1) | getbit(currbit++);
        j++;
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
    
    try {
      Assert.assertTrue(testReader.nextValue() == 0);
      Assert.assertTrue(testReader.nextValue() == 1);
      Assert.assertTrue(testReader.nextValue() == -1);
      Assert.assertTrue(testReader.nextValue() == Long.MAX_VALUE);
      Assert.assertTrue(testReader.nextValue() == Long.MAX_VALUE - 1);
      Assert.assertTrue(testReader.nextValue() == Long.MIN_VALUE + 1);
      Assert.assertTrue(testReader.nextValue() == Long.MIN_VALUE);
      
    } catch (IOException e1) {
      e1.printStackTrace();
    }
    
    try { 
      RiceCodeReader newReader = new RiceCodeReader(54,test.nbits,new FSDataInputStream(new ByteBufferInputStream(ByteBuffer.wrap(test.bits))),0,true);
      
      try {
        Assert.assertTrue(newReader.nextValue() == 0);
        Assert.assertTrue(newReader.nextValue() == 1);
        Assert.assertTrue(newReader.nextValue() == -1);
        Assert.assertTrue(newReader.nextValue() == Long.MAX_VALUE);
        Assert.assertTrue(newReader.nextValue() == Long.MAX_VALUE - 1);
        Assert.assertTrue(newReader.nextValue() == Long.MIN_VALUE + 1);
        Assert.assertTrue(newReader.nextValue() == Long.MIN_VALUE);
        
      } catch (IOException e1) {
        e1.printStackTrace();
      }
      
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
}

