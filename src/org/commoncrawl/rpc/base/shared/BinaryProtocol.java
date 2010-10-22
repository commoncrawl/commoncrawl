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

package org.commoncrawl.rpc.base.shared;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableUtils;
import org.commoncrawl.util.shared.FlexBuffer;
import org.commoncrawl.util.shared.TextBytes;

public final class BinaryProtocol  {

	private static BinaryProtocol _singleton = new BinaryProtocol();
	public static BinaryProtocol getSingleton() { return _singleton; }
	
	public static final int RAW_PROTOCOL_VERSION = 1;
	
	public static final int FIELD_ID_ENCODING_MODE_UNKNOWN = 0;
	public static final int FIELD_ID_ENCODING_MODE_SHORT = 1;
	public static final int FIELD_ID_ENCODING_MODE_VINT = 2;
	
	public static int DEFAULT_PROTOCOL_ENCODING_MODE = FIELD_ID_ENCODING_MODE_VINT;
	
	private int _currentMode;
	private static final int DEFAULT_MODE_STACK_CAPACITY = 20;
	int _currentModeIdx = -1;
	int _modeStack[] 	 = new int[DEFAULT_MODE_STACK_CAPACITY]; 
	int _nestingLevel = 0;
	
	public BinaryProtocol() {
		pushFieldIdEncodingMode(FIELD_ID_ENCODING_MODE_UNKNOWN);
	}
	
	public int popFieldIdEncodingMode() { 
		_currentMode = _modeStack[--_currentModeIdx];
		return _currentMode;
	}
		
	public void pushFieldIdEncodingMode(int mode) {
		if (_currentModeIdx + 1 == _modeStack.length) { 
			int temp[] = new int[_modeStack.length * 2];
			System.arraycopy(_modeStack, 0, temp, 0, _currentModeIdx + 1);
			_modeStack = temp;
		}
		_modeStack[++_currentModeIdx] = mode;
		_currentMode = mode;
	}
	
	public int getFieldIdEncodingMode() { 
		return _currentMode;
	}
	
	// start encoding fields 
	public void beginFields(DataOutput out) throws IOException { 
		// use default encoding type...  
		pushFieldIdEncodingMode(DEFAULT_PROTOCOL_ENCODING_MODE);
		
		// if nesting level == 0 && encoding mode is new encoding mode 
		if (_nestingLevel++ == 0 && _currentMode == FIELD_ID_ENCODING_MODE_VINT) { 
			// write out protocol version  
			out.writeByte(RAW_PROTOCOL_VERSION);
		}
	}
	
	//@Override
	public void beginField(DataOutput out, String fieldName, int fieldId) throws IOException {
		if (_currentMode == FIELD_ID_ENCODING_MODE_UNKNOWN) { 
			throw new IOException("Unknown Field Id Encoding Mode!");
		}
		if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT)
			out.writeShort(fieldId);
		else 
			WritableUtils.writeVInt(out, fieldId);
	}


	//@Override
	public void endFields(DataOutput out) throws IOException {
		if (_currentMode == FIELD_ID_ENCODING_MODE_UNKNOWN) { 
			throw new IOException("Unknown Field Id Encoding Mode!");
		}
		if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT)
			out.writeShort(-1);
		else 
			WritableUtils.writeVInt(out,-1);
		
		// ok pop encoding mode 
		popFieldIdEncodingMode();
		// reduce nesting level 
		_nestingLevel--;
	}

	//@Override
	public boolean readBool(DataInput in) throws IOException {
		return in.readBoolean();
	}

	//@Override
	public FlexBuffer readFlexBuffer(DataInput in) throws IOException {
		FlexBuffer buffer = null;
		
		int length = in.readInt();
		if (length != 0) {
			byte[] data = new byte[length];
			in.readFully(data);
			buffer = new FlexBuffer(data,0,data.length);
		}
		else { 
			buffer = new FlexBuffer();
		}
		return buffer;
	}

	//@Override
	public byte readByte(DataInput in) throws IOException {
		return in.readByte();
	}

	//@Override
	public double readDouble(DataInput in) throws IOException {
		return in.readDouble();
	}

	//@Override
	public int readFieldId(DataInput in) throws IOException {
		
		int fieldIdOut = -1;
		
		// read first byte no matter what 
		byte firstByte = in.readByte();
		// if mode is not VINT MODE   
		if (_currentMode != FIELD_ID_ENCODING_MODE_VINT) { 
			// ok if first byte is zero, then this is the old short encoding style ..  
			if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT || (firstByte == 0 || firstByte == -1)) {
				if (_currentMode == FIELD_ID_ENCODING_MODE_UNKNOWN) {
					// increment nesting level ... 
					_nestingLevel++;
					// set mode 
					pushFieldIdEncodingMode(FIELD_ID_ENCODING_MODE_SHORT);
				}
				// return second byte 
				fieldIdOut =  (int) ((((int)firstByte << 8)) | (in.readByte() & 0xff));
			}
			else if (_currentMode == FIELD_ID_ENCODING_MODE_UNKNOWN) {
				
				 
				if (_nestingLevel++ == 0) {
				// skip version byte 
					firstByte = in.readByte();
				}
				// shift to vint encoding mode ...
				pushFieldIdEncodingMode(FIELD_ID_ENCODING_MODE_VINT);
			}
		}
		
		if (_currentMode == FIELD_ID_ENCODING_MODE_VINT) { 
			// ok a little messier ... 
	    int len = WritableUtils.decodeVIntSize(firstByte);
	    if (len == 1) {
	    	fieldIdOut =  firstByte;
	    }
	    else { 
		    long i = 0;
		    for (int idx = 0; idx < len-1; idx++) {
		      byte b = in.readByte();
		      i = i << 8;
		      i = i | (b & 0xFF);
		    }
		    fieldIdOut = (int)(WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
	    }
		}
		
		if (fieldIdOut == -1) { 
			// ok pop encoding mode off stack 
			popFieldIdEncodingMode();
			// reduce nesting level ...
			_nestingLevel--;
		}
		return fieldIdOut;
	}

	//@Override
	public float readFloat(DataInput in) throws IOException {
		return in.readFloat();
	}

	//@Override
	public int readInt(DataInput in) throws IOException {
		return in.readInt();
	}

	//@Override
	public long readLong(DataInput in) throws IOException {
		return in.readLong();
	}

	//@Override
	public void readTextBytes(DataInput in,TextBytes stringBytes) throws IOException {
		if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT) { 
			// old encoding style
			int utfLen = in.readUnsignedShort();
			// ok set count in underlying object
			stringBytes.setLength(utfLen);
			// now if length != 0
			if (utfLen != 0) { 
				// access the underlying buffer object  
				FlexBuffer buffer = stringBytes.getBuffer();
				// make sure copy on write status
				buffer.copyOnWrite();
				// ok read in buytes 
				in.readFully(buffer.get(), buffer.getOffset(), utfLen);
			}
		}
		// ok read in new way 
		else { 
			stringBytes.readFields(in);
		}
	}

	//@Override
	public void writeBool(DataOutput out, boolean b) throws IOException {
		out.writeBoolean(b);
	}

	//@Override
	public void writeFlexBuffer(DataOutput out, FlexBuffer buf) throws IOException {
		out.writeInt(buf.getCount());
		if (buf.getCount() != 0) { 
			out.write(buf.get(),buf.getOffset(),buf.getCount());
		}
	}

	//@Override
	public void writeByte(DataOutput out, byte b) throws IOException {
		out.writeByte(b);
	}

	//@Override
	public void writeDouble(DataOutput out, double d) throws IOException {
		out.writeDouble(d);
	}

	//@Override
	public void writeFloat(DataOutput out, float f) throws IOException {
		out.writeFloat(f);
	}

	//@Override
	public void writeInt(DataOutput out, int i) throws IOException {
		out.writeInt(i);
	}

	//@Override
	public void writeLong(DataOutput out, long l) throws IOException {
		out.writeLong(l);
	}

	//@Override
	public void writeTextBytes(DataOutput out, TextBytes s) throws IOException {
		if (_currentMode == FIELD_ID_ENCODING_MODE_SHORT) {
			if (s.getLength() > 65535) { 
				throw new IOException("String Length Exceeds Max Encoding Length of 65535");
			}
			else { 
				// encode the old fashioned way ... 
				out.writeShort(s.getLength());
				if (s.getLength() != 0) { 
					// and the bytes 
					out.write(s.getBytes(), s.getOffset(), s.getLength());
				}
			}
		}
		else { 
			// do it the new way 
			s.write(out);
		}
	}

  public int readVInt(DataInput in) throws IOException {
    return WritableUtils.readVInt(in);
  }

  public long readVLong(DataInput in) throws IOException {
    return WritableUtils.readVLong(in);
  }

  public void writeVInt(DataOutput out, int i) throws IOException {
    WritableUtils.writeVInt(out, i);
  }

  public void writeVLong(DataOutput out, long l) throws IOException {
    WritableUtils.writeVLong(out, l);    
  }
}
