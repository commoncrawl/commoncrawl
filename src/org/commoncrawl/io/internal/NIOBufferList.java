/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.io.internal;

import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.commoncrawl.io.shared.NIODataSink;

/**
 * 
 * NIOBufferList - ByteBuffer container with separate read and write cursors
 * 
 * @author rana
 */

public final class NIOBufferList {
  
  /**
   * Min / Max Buffer Size Constants 
   */  
  private static final int MIN_BUF_SIZE = 4096;
  private static final int MAX_BUF_SIZE = MIN_BUF_SIZE << 4;

  /**
   * Data Members 
   * 
   */  
  
  
  /** Buffer Queue */
  private LinkedList<ByteBuffer> _bufferList = new LinkedList<ByteBuffer>();
  /** size in bytes of the buffers in the buffer list */
  private int _bufferListBytes =0;
  /** Active Read Buffer Pointer */
  private ByteBuffer             _readBuffer = null;
  /** Active Write Buffer Pointer */
  private ByteBuffer             _writeBuffer = null;
  /** Last Allocated ByteBuffer Size  */
  private int                    _lastWriteBufSize = 0;
  /** Minimum / Maximum Buffer Sizes */
  private int                    _minBufferSize  = MIN_BUF_SIZE;
  private int                    _maxBufferSize  = MAX_BUF_SIZE;
  /** blocking consumer support **/
  private NIODataSink _consumer = null;
  /** read event **/
  private ReentrantLock _readLock = null;
  private Condition      _readEvent = null;
  
  /** get set consumer **/
  public synchronized NIODataSink getSink() { 
    return _consumer;
  }

  public synchronized void setSink(NIODataSink consumer) { 
    _consumer = consumer;
  }
  
  public synchronized void setReadEvent(ReentrantLock lock, Condition readEvent) { 
    _readLock  = lock;
    _readEvent = readEvent;
  }
  
  
  /** Set Min/Max Buffer Size */
  public void setMinBufferSize(int size) { _minBufferSize = size; }
  public void setMaxBufferSize(int size) { _maxBufferSize = size; }
  
  /** Reset State (Release buffers) */
  public synchronized void reset() { 
    _bufferList.clear();
    _readBuffer = null;
    _writeBuffer = null;
  }

  /** Returns true if there is data available (to be read) in the buffer */
  public synchronized boolean isDataAvailable() { 
    if (_readBuffer != null && _readBuffer.remaining() != 0 )
      return true;
    else {
      return _bufferListBytes != 0;
    }
  }
  
  /** Returns the number of readable bytes */
  public synchronized int available() { 
    
    int size = 0;
    if (_readBuffer != null) {
      size += _readBuffer.remaining();
    }
    
  	size += _bufferListBytes;

  	return size;
  }
  
  /* Skip ahead from the current Read Cursor position */
  public long skip(long skipAmount)throws IOException { 
    
    long amountSkipped = 0;

    // retrieve the initial buffer ... 
    ByteBuffer readBuffer = _getNextReadBuf();
    
    while (skipAmount != 0) { 

      if (readBuffer == null) {
        break;
      }

      long bytesAvailable = Math.min(skipAmount,(long)readBuffer.remaining()); 

      readBuffer.position(readBuffer.position() + (int)bytesAvailable);
      
      amountSkipped += bytesAvailable;
      skipAmount    -= bytesAvailable;
      
      readBuffer = _getNextReadBuf();      
    }
    return (amountSkipped != 0) ? amountSkipped : -1;
  }

  public enum CRLFReadState { 
    NONE,
    GOT_CR,
    DONE
  }
  
  public CRLFReadState readCRLFLine(StringBuffer accumulator, int lineMax,CRLFReadState lastReadState) throws IOException { 

    ByteBuffer currentBuffer = null;
    
    while (lastReadState != CRLFReadState.DONE && (currentBuffer = _getNextReadBuf()) != null) {
      
      // read up to the end of the buffer ... 
      while (currentBuffer.position() < currentBuffer.limit()) { 
        
        char currentChar = (char) currentBuffer.get();
        
        if ((lastReadState != CRLFReadState.GOT_CR && currentChar == '\r')) { 
          lastReadState = CRLFReadState.GOT_CR;
        }
        else if (lastReadState == CRLFReadState.GOT_CR && currentChar == '\n') {
          return CRLFReadState.DONE;
        }
        else {
          lastReadState = CRLFReadState.NONE;
          // add it to the buffer ...
          accumulator.append(currentChar);
          
          if (accumulator.length() > lineMax) {             
            throw new IOException("Line Size Limit Reached With No Terminator!");
          }
        }
      }
    }
    return lastReadState;
  }
  
 
  /** read into a pre-allocated byte buffer */
  public int read(byte[] buffer)throws IOException{ 
    return read(buffer,0,buffer.length);
  }
  
  /** read into a pre-allocated byte buffer 
   * 
   * @param offset the offset (in the byte buffer) where data should be stored
   * @param count  the maximum number of bytes to store in the byte buffer
   * @return
   * @throws IOException
   */
  public int read(byte[] buffer,int offset, int count) throws IOException {
    
    int bytesRead = 0;
    
    // retrieve the initial buffer ... 
    ByteBuffer readBuffer = _getNextReadBuf();
    
    while (count > 0) { 
     
      if (readBuffer == null) {
        break;
      }
      int bytesAvailable = Math.min(count,readBuffer.remaining()); 
      
      readBuffer.get(buffer,offset,bytesAvailable);
      
      bytesRead += bytesAvailable;
     
      offset += bytesAvailable;
      count -= bytesAvailable;
      
      readBuffer = _getNextReadBuf();
    }
    
    return (bytesRead != 0) ? bytesRead : -1; 
  }

  /** put back (at the top of the buffer queue) a previously read byte buffer */
  public synchronized void  putBack(ByteBuffer existingReadBuffer)throws IOException  {
    
    if (existingReadBuffer == null)
      throw new IOException("Invalid Call to putBack - incoming Buffer is null!");
    if (_readBuffer != null) {
      if (_readBuffer == existingReadBuffer) { 
        throw new IOException("Invalid Call to putBack - Trying to put back current read buffer!");
      }
      if (_readBuffer == _writeBuffer) { 
        throw new RuntimeException("Critical Error!! read and write buffer pointers identical !!!");
      }
      // update buffer list bytes ... 
      _bufferListBytes += _readBuffer.remaining();
      // put back the next buffer ...
      _bufferList.addFirst(_readBuffer);
    }
    _readBuffer = existingReadBuffer;
    if (_readBuffer == _writeBuffer) { 
      throw new RuntimeException("Critical Error!! read and write buffer pointers identical !!!");
    }
  }
  
  /** get the next ByteBuffer worth of data 
   * 
   * @param desiredMinSize the minimum size of available data required in the ByteBuffer
   * @return
   * @throws IOException
   */ 
  public ByteBuffer read(int desiredMinSize) throws IOException { 
    
    // get top buffer in queue 
    ByteBuffer bufferOut = read();
    
    if (bufferOut != null) { 
    
      int shortBy = Math.max(0,desiredMinSize - bufferOut.remaining());
    
      // if less than min size ... 
      if (shortBy != 0) { 
        
        // check to see if subsequent buffer has remainder ... 
        ByteBuffer nextBuffer = _getNextReadBuf();
        
        if (nextBuffer != null) {
          // compact the existing buffer ... 
          bufferOut.compact();
          // if next buffer has remainder ...
          // and source buffer can accept it ... 
          if (bufferOut.capacity() - bufferOut.limit() >= shortBy && nextBuffer.remaining() >= shortBy) { 
            // increase limit ... 
            int originalLimit = bufferOut.limit();
            int newLimit      = originalLimit + shortBy; 
            bufferOut.limit(newLimit);
            
            for (;originalLimit<newLimit;++originalLimit) {
              bufferOut.put(originalLimit,nextBuffer.get());
            }
          }
        }
      }
    }
    return bufferOut;
  }
  
  /** obtain the next readable ByteBuffer */
  public synchronized ByteBuffer read()throws IOException {
    _getNextReadBuf();
    ByteBuffer bufferOut = _readBuffer;
    _readBuffer = null;
    return bufferOut;
  }
  
  /* peek at ByteBuffer - don't get */
  public synchronized ByteBuffer peekAtWriteBuffer(){ 
    return _writeBuffer;
  }
  
  /* add a previously written buffer to the tail of the read queue */
  public synchronized void write(ByteBuffer buffer) throws IOException {
    // NOTE: buffer should already be flipped for READ 
    
    // flush existing open buffer ...
    if (_writeBuffer != buffer) {
      flush();
    }
    
    // now set this as the current write buffer
    _writeBuffer = buffer;
  }
  
  /* write a byte value */
  public void write(int value) throws IOException {
    
    ByteBuffer writeBuffer = getWriteBuf();
    writeBuffer.put((byte)value);
  }
  
  /* copy the specified byte buffer into a ByteBuffer and add to the read queue 
   * 
   * @param offset  offset into the byte buffer
   * @param size    size of the byte buffer
   * 
   */ 
  public void write(byte[] buffer, int offset, int size) throws IOException { 
    
    while (size > 0) {
      
      ByteBuffer writeBuffer = getWriteBuf();
      
      int available = Math.min(writeBuffer.remaining(),size);
      
      writeBuffer.put(buffer,offset,available);
      
      offset += available;
      size   -= available;
    }
  }
  
  /** internal - get the next read buffer from the queue */
  private synchronized ByteBuffer _getNextReadBuf() throws IOException { 
    
    while (_readBuffer == null || _readBuffer.hasRemaining() == false) {

      _readBuffer = null;
      
      // once we reach here... we can recover the next buffer in the list ... 
      if (!_bufferList.isEmpty()) {
        _readBuffer = _bufferList.removeFirst();
        _bufferListBytes -= _readBuffer.remaining();
      }
      // now if the buffer is null break out ... 
      if (_readBuffer == null) { 
        break;
      }
    }
    
    if (_readBuffer != null && _readEvent != null) { 
      _readLock.lock();
      
      // signal the read event, thus potentially waking up a sleeping writer ... 
      _readEvent.signal();
      _readEvent = null;
      
      _readLock.unlock();
    }
    
    return _readBuffer;
  }
 
  /** allocate a new byte buffer (or potentially retrieve a buffer from the pool )*/
  public final ByteBuffer allocateBuffer() throws IOException {

    int desiredAllocSize = Math.max(_minBufferSize, Math.min(_maxBufferSize, _lastWriteBufSize << 1));
    return ByteBuffer.allocate(desiredAllocSize);
  }
  
  /** get a write buffer   
   *   
   * @return a ByteBuffer the caller can write into. Note: NIOBufferList OWNS the buffer, and will flush it when appropriate. 
   *                 Caller only writes into it.
   * @throws IOException
   */
  public synchronized ByteBuffer getWriteBuf() throws IOException {
    
    if (_writeBuffer == null || _writeBuffer.hasRemaining() == false) { 
      
      flush();
      
      _writeBuffer = allocateBuffer();
      
    }
    return _writeBuffer;
  }
  
  /** flush any partial writes and add the resulting ByteBuffer to the read queue */
  public void flush() { 
	
    NIODataSink sink = null;
    ByteBuffer lastWriteBuffer = null;
    
    synchronized (this) {
      if (_writeBuffer != null && _writeBuffer.position() != 0) { 
      
        _lastWriteBufSize = Math.max(_minBufferSize,_writeBuffer.position());
        
        // get the buffer ready for a read ... 
        _writeBuffer.flip();
        if (_readBuffer == _writeBuffer) { 
          throw new RuntimeException("read and write buffer pointers identical !!!");
        }
        
        // now ...  tricky ... if blocking consumer is specified ... delegate buffer queuing to it ..
        sink = getSink();
        if (sink != null) {
          lastWriteBuffer = _writeBuffer;
        }
        else {
          // increment queued bytes count ... 
          _bufferListBytes += _writeBuffer.remaining();
          // and add to our internal list ... 
          _bufferList.add(_writeBuffer);
        }
       
        // either way clear our reference to the buffer ... 
        _writeBuffer = null;
      }
    }
    
    if (sink != null && lastWriteBuffer != null) { 
      // and pass on the buffer to the consumer ... 
      getSink().available(lastWriteBuffer);
    }
    
  }
  
}
