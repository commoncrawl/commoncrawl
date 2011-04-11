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

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.BufferUnderflowException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Seekable;

/**
 * some utility classes to do memory mapped io in java 
 * 
 * @author rana
 *
 */
public class MMapUtils {

  
  static final Log LOG = LogFactory.getLog(MMapUtils.class);
  
  public static final int DEFAULT_MAX_BUFF = 256 * 1024;

  /**
   * <code>true</code>, if this platform supports unmapping mmapped files.
   */
  public static final boolean UNMAP_SUPPORTED;
  
  static {
    boolean v;
    try {
      Class.forName("sun.misc.Cleaner");
      Class.forName("java.nio.DirectByteBuffer")
        .getMethod("cleaner");
      v = true;
    } catch (Exception e) {
      v = false;
    }
    UNMAP_SUPPORTED = v;
  }
  
  /**
   * Returns <code>true</code>, if the unmap workaround is enabled.
   * @see #setUseUnmap
   */
  public static boolean getUseUnmap() {
    return UNMAP_SUPPORTED;
  }
  
  /**
   * Try to unmap the buffer, this method silently fails if no support
   * for that in the JVM. On Windows, this leads to the fact,
   * that mmapped files cannot be modified or deleted.
   */
  final static void cleanMapping(final ByteBuffer buffer) throws IOException {
    if (getUseUnmap()) {
      try {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Object>() {
          public Object run() throws Exception {
            final Method getCleanerMethod = buffer.getClass()
              .getMethod("cleaner");
            getCleanerMethod.setAccessible(true);
            final Object cleaner = getCleanerMethod.invoke(buffer);
            if (cleaner != null) {
              cleaner.getClass().getMethod("clean")
                .invoke(cleaner);
            }
            return null;
          }
        });
      } catch (PrivilegedActionException e) {
        final IOException ioe = new IOException("unable to unmap the mapped buffer");
        ioe.initCause(e.getCause());
        throw ioe;
      }
    }
  }
    

  /**
   * Returns the current mmap chunk size.
   * @see #setMaxChunkSize
   */
  public static int getMaxChunkSize() {
    return DEFAULT_MAX_BUFF;
  }
  
  public static class MMapFile { 
    
    long length = -1;
    ByteBuffer buffers[]=null;
    int        bufSizes[] = null;
    private int refCount =0;
    private boolean closePending = false;
    
    public MMapFile(File input) throws IOException { 
      RandomAccessFile raf = new RandomAccessFile(input, "r");
      try {
        this.length = raf.length();
        
        
        if ((length / getMaxChunkSize()) > Integer.MAX_VALUE)
          throw new IllegalArgumentException
            ("RandomAccessFile too big for maximum buffer size: "
             + raf.toString());
        
        int nrBuffers = (int) (length / getMaxChunkSize());
        if (((long) nrBuffers * getMaxChunkSize()) <= length) nrBuffers++;
        
        this.buffers = new ByteBuffer[nrBuffers];
        this.bufSizes = new int[nrBuffers];
        
        long bufferStart = 0;
        FileChannel rafc = raf.getChannel();
        for (int bufNr = 0; bufNr < nrBuffers; bufNr++) { 
          int bufSize = (length > (bufferStart + getMaxChunkSize()))
            ? getMaxChunkSize()
            : (int) (length - bufferStart);
          this.buffers[bufNr] = rafc.map(MapMode.READ_ONLY,bufferStart,bufSize);
          this.bufSizes[bufNr] = bufSize;
          bufferStart += bufSize;
        }
        LOG.info("Initialized MapFile from file:" + input.getAbsolutePath() +  " NumBuffers:" + buffers.length + " TotalLength:" + this.length);
      } finally {
        raf.close();
      }
    }
    
    public FSDataInputStream newInputStream() throws IOException { 
      FSInputStream stream = new MMapFileInputStream();
      FSDataInputStream dataStream = new FSDataInputStream(stream);
      return dataStream;
    }
    
    private synchronized void addRef() { 
      refCount++;
    }
    
    private synchronized void release() { 
      if (--refCount == 0 && closePending) { 
        try {
          close();
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    }
    
    public synchronized void close()throws IOException {
      if (refCount == 0) {
        if (buffers != null) { 
          for (ByteBuffer buffer : buffers) { 
            cleanMapping(buffer);
          }
        }
        buffers = null;
      }
      else { 
        closePending = true;
      }
    }
    
    public long getLength() { 
      return length;
    }
    
    
    // Because Java's ByteBuffer uses an int to address the
    // values, it's necessary to access a file >
    // Integer.MAX_VALUE in size using multiple byte buffers.
    public class MMapFileInputStream extends FSInputStream {
    
      private int curBufIndex = 0;
      private final int maxBufSize = getMaxChunkSize();
      private ByteBuffer curBuf; // redundant for speed: buffers[curBufIndex]
      
      public MMapFileInputStream() throws IOException {
        addRef();
        seek(0L);
      }
    
      @Override
      public int read() throws IOException {
        try {
          return curBuf.get() & 0xff;
        } catch (BufferUnderflowException e) {
          curBufIndex++;
          if (curBufIndex >= buffers.length)
            throw new IOException("read past EOF");
          curBuf = buffers[curBufIndex].slice();
          curBuf.position(0);
          return curBuf.get() & 0xff;
        }
      }
    
      @Override
      public int read(byte[] bytes, int offset, int len) throws IOException {
        try {
          curBuf.get(bytes, offset, len);
          return len;
        } catch (BufferUnderflowException e) {
          int bytesRead = 0;
          int curAvail = curBuf.remaining();
          while (len > curAvail) {
            curBuf.get(bytes, offset, curAvail);
            bytesRead += curAvail;
            len -= curAvail;
            offset += curAvail;
            curBufIndex++;
            if (curBufIndex >= buffers.length) {
              return bytesRead;
            }
            curBuf = buffers[curBufIndex].slice();
            curBuf.position(0);
            curAvail = curBuf.remaining();
          }
          curBuf.get(bytes, offset, len);
          return bytesRead + len;
        }
      }
      
      @Override
      public void close() throws IOException {
        release();
      }
      
      @Override
      public int available() throws IOException {
        long amtAvailable = (length() - getPos());
        return (amtAvailable <= Integer.MAX_VALUE) ? (int)amtAvailable : Integer.MAX_VALUE;
      };
    
      @Override
      public void seek(long pos) throws IOException {
        int bufferIndex = (int) (pos / maxBufSize);
        if (curBuf == null || bufferIndex != curBufIndex) { 
          curBufIndex = bufferIndex;
          curBuf = buffers[curBufIndex].slice();
        }
        int bufOffset = (int) (pos - ((long) curBufIndex * maxBufSize));
        curBuf.position(bufOffset);
      }
    
      public long length() {
        return length;
      }

      @Override
      public long getPos() throws IOException {
        return ((long) curBufIndex * maxBufSize) + curBuf.position();
      }

      @Override
      public boolean seekToNewSource(long targetPos) throws IOException {
        seek(targetPos);
        return false;
      }

      
      public short readShort() throws IOException {
        try {
          return curBuf.getShort();
        } catch (BufferUnderflowException e) {
          return (short) (((read() & 0xFF) <<  8) |  (read() & 0xFF));
        }
      }

      public int readInt() throws IOException {
        try {
          return curBuf.getInt();
        } catch (BufferUnderflowException e) {
          return ((read() & 0xFF) << 24) | ((read() & 0xFF) << 16)
          | ((read() & 0xFF) <<  8) |  (read() & 0xFF);
        }
      }

      public long readLong() throws IOException {
        try {
          return curBuf.getLong();
        } catch (BufferUnderflowException e) {
          return (((long)readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
        }
      }
      
      /** Reads an int stored in variable-length format.  Reads between one and
       * five bytes.  Smaller values take fewer bytes.  Negative numbers are not
       * supported.
       * @see DataOutput#writeVInt(int)
       */
      public int readVInt() throws IOException {
        int b = read();
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
          b = read();
          i |= (b & 0x7F) << shift;
        }
        return i;
      }
      
      /** Reads a long stored in variable-length format.  Reads between one and
       * nine bytes.  Smaller values take fewer bytes.  Negative numbers are not
       * supported. */
      public long readVLong() throws IOException {
        int b = read();
        long i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
          b = read();
          i |= (b & 0x7FL) << shift;
        }
        return i;
      }

    
    }    
  }
  
}
