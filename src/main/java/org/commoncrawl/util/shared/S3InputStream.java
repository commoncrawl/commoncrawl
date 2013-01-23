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
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.Callbacks;
import org.commoncrawl.io.internal.NIOBufferList;
import org.commoncrawl.io.internal.NIOBufferListInputStream;
import org.commoncrawl.io.internal.NIOHttpConnection;

/** 
 * 
 * An InputStream that fetches data from S3 by using an
 * S3Downloader instance to fetch/buffer data in a background thread.
 *
 * @author rana
 *
 */
public class S3InputStream extends NIOBufferListInputStream implements S3Downloader.Callback {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(S3InputStream.class);
  
  URI uri;
  S3Downloader downloader = null;
  AtomicReference<Exception> _exception = new AtomicReference<Exception>();
  ReentrantLock _writeLock = new ReentrantLock();
  AtomicReference<Condition>     _writeEvent = new AtomicReference<Condition>(_writeLock.newCondition());
  AtomicBoolean _eofCondition = new AtomicBoolean();
  AtomicReference<NIOHttpConnection> pausedConnection = new AtomicReference<NIOHttpConnection>();
  int MAX_BUFFER_SIZE = 1048576;

  
  /** 
   * Initiate the stream with specified s3/s3n uri. 
   * @param uri s3/s3n uri that points to an s3 object 
   * @param s3AccessKey  
   * @param s3Secret 
   * @param bufferSize set this to be at least 1MB or higher to ensure decent performance 
   * @throws IOException
   */
  public S3InputStream(URI uri,String s3AccessKey,String s3Secret,int bufferSize) throws IOException { 
    super(new NIOBufferList());
    
    this.uri = uri;
    
    downloader = new S3Downloader(uri.getHost(), s3AccessKey,s3Secret, false);
    // we are download a single stream ... 
    downloader.setMaxParallelStreams(1);
    // initialize the callback 
    downloader.initialize(this);
    // initiate the download 
    LOG.info("Fetching:" + uri.getPath());
    downloader.fetchItem(uri.getPath().substring(1));    
  }
  
  
  @Override
  protected void ensureBuffer() throws IOException {
    super.ensureBuffer();
    while(_activeBuf == null) {
      
      //System.out.println("Read from Main Thread  for Path:" + uri + ". Checking for EOF or Error");
      _writeLock.lock();
      try { 
        if (_eofCondition.get()) {
          if (_exception.get() != null) { 
            LOG.error("Read from Main Thread for Path:" + uri + " detected Exception"); 
            throw new IOException(_exception.get());
          }
          else {
            LOG.info("Read from Main Thread for Path:" + uri + " detected EOF");
            return;
          }
        }
        else { 
          _writeEvent.set(_writeLock.newCondition());
          //long nanoTimeStart = System.nanoTime();
          //System.out.println("Read from Main Thread for Path:" + uri + " Waiting on Write");
          try {
            _writeEvent.get().await();
            //long nanoTimeEnd = System.nanoTime();
            //System.out.println("Read from Main Thread for Path:" + uri + " Returned from Wait Took:" + (nanoTimeEnd-nanoTimeStart));
          } catch (InterruptedException e) {
            LOG.error("Read from Main Thread for Path:" + uri + " was Interrupted. Exiting");
            throw new IOException(e);
          }
        }
      }
      finally { 
        _writeLock.unlock();
      }
      super.ensureBuffer();
    }
    if (pausedConnection.get() != null && _bufferQueue.available() < MAX_BUFFER_SIZE) {
      final NIOHttpConnection connection = pausedConnection.get();
      pausedConnection.set(null);
      downloader.getEventLoop().queueAsyncCallback(new Callbacks.Callback() {

        @Override
        public void execute() {
          System.out.println("*** RESUMING DOWNLOADS ***");
          try {
            connection.enableReads();
          } catch (IOException e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
        } 
        
      });
    }
  }
  
  @Override
  public void close() throws IOException {
    downloader.shutdown();
  }  
  
  @Override
  public boolean downloadStarting(int itemId, String itemKey,int contentLength) {
    return true;
  }

  @Override
  public boolean contentAvailable(NIOHttpConnection theConnection,int itemId, String itemKey,NIOBufferList contentBuffer) {
    
    ByteBuffer buffer = null;
    IOException exception = null;
    //int receivedBytes = 0;
    try { 
      while ((buffer  = contentBuffer.read()) != null) {
        if (buffer.position() != 0) { 
          buffer = buffer.slice();
        }
        //receivedBytes += buffer.remaining();
        buffer.position(buffer.limit());
        _bufferQueue.write(buffer);
      }
      _bufferQueue.flush();
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      exception = e;
    }
    if (_bufferQueue.available() >= MAX_BUFFER_SIZE) {
      theConnection.disableReads();
      pausedConnection.set(theConnection);
    }
    //long nanoTimeStart = System.nanoTime();
    _writeLock.lock();
    //long nanoTimeEnd = System.nanoTime();
    //System.out.println("Received: " + receivedBytes + "for URI:" + uri + " Lock took:" + (nanoTimeEnd-nanoTimeStart));
    try { 
      Condition writeCondition = _writeEvent.getAndSet(null);
      if (exception != null) { 
        _eofCondition.set(true);
        _exception.set(exception);
      }
      if (writeCondition != null) { 
        writeCondition.signal();
      }
    }
    finally { 
      _writeLock.unlock();
    }
    return true;
  }

  @Override
  public void downloadFailed(int itemId, String itemKey, String errorCode) {
    LOG.error("Download Failed for URI:" + S3InputStream.this.uri);
    _writeLock.lock();
    try {
      _exception.set(new IOException(errorCode));
      _eofCondition.set(true);
      Condition writeCondition = _writeEvent.getAndSet(null);
      if (writeCondition != null) { 
        writeCondition.signal();
      }
    }
    finally { 
      _writeLock.unlock();
    }
  }

  @Override
  public void downloadComplete(int itemId, String itemKey) {
    LOG.info("Download Complete for URI:" + S3InputStream.this.uri);
    _writeLock.lock();
    try {
      _exception.set(null);
      _eofCondition.set(true);
      Condition writeCondition = _writeEvent.getAndSet(null);
      if (writeCondition != null) { 
        writeCondition.signal();
      }
    }
    finally { 
      _writeLock.unlock();
    }
  }  
}
