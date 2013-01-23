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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class NIOBufferListInputStream extends InputStream {

  protected NIOBufferList 	_bufferQueue = null;
	protected ByteBuffer	    _activeBuf = null;
	protected long            _streamPos = 0;
	
	public NIOBufferListInputStream(NIOBufferList source) { 
		_bufferQueue = source;
	}
	
	
	public synchronized void reset() { 
		_activeBuf = null;
	}
	
	
	public long getStreamPos() { 
	  return _streamPos;
	}
	
	protected synchronized  void getNext() throws IOException { 
		_activeBuf = null;
		_activeBuf = _bufferQueue.read();
	}
	
	protected synchronized void ensureBuffer() throws IOException { 
		if (_activeBuf == null || _activeBuf.remaining() == 0) { 
			getNext();
		}
	}

	@Override
	public synchronized int read() throws IOException {
	    ensureBuffer();
		if (_activeBuf != null) {
		  _streamPos++;
			return (int) (_activeBuf.get() & 0xff);
		}
		return -1;
	}

	@Override
	public synchronized int available() throws IOException {
		
		int available = 0;
		
		if (_activeBuf != null) { 
			available += _activeBuf.remaining();
		}
		available += _bufferQueue.available();
		
		return available;
	}
	
	/**
	 * create and return  a substream of a desired size - consume desired size
	 * bytes from source stream.
	 * 
	 * 
	 * @param desiredStreamSize - Desired Stream Size. If current stream
	 * contains less than disired size bytes an EOFException is thrown.
	 * @return new NIOBufferListInputStream contstrained to desired stream size
	 * @throws IOException
	 */
	public synchronized NIOBufferListInputStream subStream(int desiredStreamSize)throws IOException { 
	  // throw EOF if we don't have enough bytes to service the request 
	  if (available() < desiredStreamSize) { 
	    throw new EOFException();
	  }
	  // otherwise ... allocate a new buffer list ...  
	  NIOBufferList bufferList = new NIOBufferList();
	  
	  int sizeOut = 0;
	  int len = desiredStreamSize;
	  
	  // walk buffers from existing stream + source buffer list ... 
      while (len != 0) { 
          // grab new ByteBuffer from buffer list if necessary ... 
          ensureBuffer();
          
          //if we could get another buffer from list. bail... 
          if (_activeBuf == null) { 
              break;
          }
          
          // calculate bytes available
          final int sizeAvailable = _activeBuf.remaining();
          // and bytes to read this iteration ... 
          final int sizeToRead    = Math.min(sizeAvailable,len);
          // if we can consume entire buffer ... 
          if (sizeAvailable <= sizeToRead) {
            // slice the existing buffer ... 
            ByteBuffer buffer = _activeBuf.slice();
            // position it to limit (bufferList.write flips it, so we must do this).
            buffer.position(buffer.limit());
            // add to buffer list via write 
            bufferList.write(buffer);
            // null out this buffer as it has been fully consumed 
            _activeBuf = null;
          }
          else {
            // slice the existing buffer 
            ByteBuffer buffer = _activeBuf.slice();
            // reset limit on new buffer 
            buffer.limit(buffer.position() + sizeToRead);
            // position the new buffer to limit (to facilitate flip in write call)
            buffer.position(buffer.limit());
            // add it to buffer list 
            bufferList.write(buffer);
            // and increment position of source buffer 
            _activeBuf.position(_activeBuf.position() + sizeToRead);
          }
          
          len -= sizeToRead;
          sizeOut += sizeToRead;
      }
      // flush any trailing write buffer in new list ... 
      bufferList.flush();
      
      if (sizeOut != desiredStreamSize) {
        throw new EOFException();
      }
      
      _streamPos += sizeOut;
      
      return new NIOBufferListInputStream(bufferList);	  
	}

	@Override
	public synchronized int read(byte[] b, int off, int len) throws IOException {
		
		int sizeOut = 0;
		
		while (len != 0) { 
			
			ensureBuffer();
			
			if (_activeBuf == null) { 
				break;
			}
			
			final int sizeAvailable = _activeBuf.remaining();
			final int sizeToRead    = Math.min(sizeAvailable,len);
			
			_activeBuf.get(b, off, sizeToRead);
			
			len -= sizeToRead;
			sizeOut += sizeToRead;
			off += sizeToRead;
		}
		
		_streamPos += sizeOut;
		
		return (sizeOut != 0) ? sizeOut : -1;
	}

	@Override
	public synchronized int read(byte[] b) throws IOException {
		return read(b,0,b.length);
	}

	@Override
	public long skip(long skipAmount) throws IOException {
		
		long skipAmountOut = 0;
		
		while (skipAmount != 0) { 
			
			ensureBuffer();
			
			if (_activeBuf == null) { 
				break;
			}
			
			final long sizeAvailable = _activeBuf.remaining();
			final int  sizeToSkip    = (int) Math.min(sizeAvailable,skipAmount);
			
			_activeBuf.position(_activeBuf.position() + sizeToSkip);
			
			skipAmount -= sizeToSkip;
			skipAmountOut += sizeToSkip;
		}
		
		_streamPos += skipAmountOut;
		
		return skipAmountOut;
	}
	
	@Override
	public synchronized void close() throws IOException {
	  
	  if (_activeBuf != null && _activeBuf.remaining() != 0) { 
	    if (_bufferQueue != null) { 
	      _bufferQueue.putBack(_activeBuf);
	    }
	  }
	  
	  super.close();
	}

}
