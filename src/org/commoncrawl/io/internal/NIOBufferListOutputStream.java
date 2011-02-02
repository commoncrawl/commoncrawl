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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;


// a simple wrapper to write data directly into byte buffers ... 
public class NIOBufferListOutputStream extends OutputStream {

	private NIOBufferList 	_target  = null;
	private ByteBuffer	_buffer  = null;
		
	
	public NIOBufferListOutputStream(NIOBufferList buffer) { 
		_target = buffer;
	}
	
	//@Override
	public void write(int b) throws IOException {
		if (capacity() == 0)
			grow();
		_buffer.put((byte)b);
	}

	//@Override
	public void write(byte[] b, int off, int len) throws IOException {
		while (len != 0) { 
			if (capacity() == 0)
				grow();
			
			// calculate size to write based on buffer capacity 
			final int sizeAvailable = _buffer.remaining();
			final int sizeToWrite   = Math.min(sizeAvailable,len);
			
			// write data ... 
			_buffer.put(b,off,sizeToWrite);
			
			// and increment pointers ...
			off += sizeToWrite;
			len -= sizeToWrite;
		}	
	}

	//@Override
	public void write(byte[] b) throws IOException {
		write(b,0,b.length);
	}

	private final int  capacity() {
		return (_buffer != null) ? _buffer.remaining()  : 0;
	}
	private final void grow() throws IOException {
		synchronized(this) { 
			
			if (_buffer != null) { 
	 			_target.write(_buffer);
			}
			_buffer = _target.getWriteBuf();
		}	
	}

	//@Override
	public void flush() throws IOException {
		if (_buffer != null && _buffer.position() != 0) { 
			_target.write(_buffer);
			_buffer = null;			
		}
		_target.flush();
	}
	
	public void reset() { 
		_buffer = null;
	}
}
