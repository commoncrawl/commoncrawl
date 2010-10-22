package org.commoncrawl.util.shared;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *  A collection of utility methods for working on GZIPed data.
 */
public class GZIPUtils {
  
  private static final Log LOG = LogFactory.getLog(GZIPUtils.class);
  private static final int EXPECTED_COMPRESSION_RATIO= 5;
  private static final int BUF_SIZE= 4096;

  
  public static class UnzipResult { 

    public UnzipResult(byte[] data,boolean truncated) { 
      this.data = data;
      this.wasTruncated = truncated;
    }
    
    public    byte[]    data;
    public    boolean   wasTruncated = false;
  }
  
  /**
   * Returns an gunzipped copy of the input array.  If the gzipped
   * input has been truncated or corrupted, a best-effort attempt is
   * made to unzip as much as possible.  If no data can be extracted
   * <code>null</code> is returned.
   */
  public static final UnzipResult unzipBestEffort(byte[] in) {
     return unzipBestEffort(in,0,in.length, Integer.MAX_VALUE);
  }

  public static final UnzipResult unzipBestEffort(byte[] in,int sizeLimit) {
  	return unzipBestEffort(in, 0, in.length, sizeLimit);
  }
  
  /**
   * Returns an gunzipped copy of the input array, truncated to
   * <code>sizeLimit</code> bytes, if necessary.  If the gzipped input
   * has been truncated or corrupted, a best-effort attempt is made to
   * unzip as much as possible.  If no data can be extracted
   * <code>null</code> is returned.
   */
  public static final UnzipResult unzipBestEffort(byte[] in,int offset,int sizeIn, int sizeLimit) {
  	
    try {
      // decompress using GZIPInputStream 
      ByteArrayOutputStream outStream = 
        new ByteArrayOutputStream(EXPECTED_COMPRESSION_RATIO * in.length);

      boolean truncated = false;
      
      GZIPInputStream inStream = 
        new GZIPInputStream ( new ByteArrayInputStream(in,offset,sizeIn) );

      byte[] buf = new byte[BUF_SIZE];
      int written = 0;
      while (true) {
        try {
          int size = inStream.read(buf);
          if (size <= 0) 
            break;
          if ((written + size) > sizeLimit) {
            outStream.write(buf, 0, sizeLimit - written);
            truncated = true;
            break;
          }
          outStream.write(buf, 0, size);
          written+= size;
        } catch (Exception e) {
          break;
        }
      }
      try {
        outStream.close();
      } catch (IOException e) {
      }

      return new UnzipResult(outStream.toByteArray(),truncated);

    } catch (IOException e) {
      return null;
    }
    catch (OutOfMemoryError e) { 
    	LOG.fatal(CCStringUtils.stringifyException(e));
    	return null;
    }
  }


  /**
   * Returns an gunzipped copy of the input array.  
   * @throws IOException if the input cannot be properly decompressed
   */
  public static final byte[] unzip(byte[] in) throws IOException {
    // decompress using GZIPInputStream 
    ByteArrayOutputStream outStream = 
      new ByteArrayOutputStream(EXPECTED_COMPRESSION_RATIO * in.length);

    GZIPInputStream inStream = 
      new GZIPInputStream ( new ByteArrayInputStream(in) );

    byte[] buf = new byte[BUF_SIZE];
    while (true) {
      int size = inStream.read(buf);
      if (size <= 0) 
        break;
      outStream.write(buf, 0, size);
    }
    outStream.close();

    return outStream.toByteArray();
  }

  /**
   * Returns an gzipped copy of the input array.
   */
  public static final byte[] zip(byte[] in) {
    try {
      // compress using GZIPOutputStream 
      ByteArrayOutputStream byteOut= 
        new ByteArrayOutputStream(in.length / EXPECTED_COMPRESSION_RATIO);

      GZIPOutputStream outStream= new GZIPOutputStream(byteOut);

      try {
        outStream.write(in);
      } catch (Exception e) {
        LOG.warn(CCStringUtils.stringifyException(e));
      }

      try {
        outStream.close();
      } catch (IOException e) {
        LOG.warn(CCStringUtils.stringifyException(e));
      }

      return byteOut.toByteArray();

    } catch (IOException e) {
      LOG.warn(CCStringUtils.stringifyException(e));
      return null;
    }
  }
    
}
