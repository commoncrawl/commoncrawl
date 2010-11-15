package org.commoncrawl.util.shared;

/*
 *    Copyright 2010 - CommonCrawl Foundation
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * A tweaking of the hadoop Buffer class, basically allowing for cheap buffer
 * copies (via underlying storage sharing), some copy on write support, and most
 * importantly, support of offset based backing buffers
 * 
 * A byte sequence that is used as a Java native type for buffer. It is
 * resizable and distinguishes between the count of the seqeunce and the current
 * capacity.
 * 
 */
public final class FlexBuffer implements WritableComparable, Cloneable {
  /** Number of valid bytes in this.bytes. */
  private int     count;
  /** Backing store for Buffer. */
  private byte[]  zbytes    = null;
  /** offset - optional **/
  private int     offset    = 0;
  /** shared backing array or not **/
  private boolean _isShared = false;

  /**
   * Create a zero-count sequence.
   */
  public FlexBuffer() {
    this.count = 0;
  }

  /**
   * is the underlying byte buffer shared
   * 
   */
  public boolean isShared() {
    return _isShared;
  }

  public FlexBuffer(FlexBuffer other) {
    count = other.count;
    zbytes = other.zbytes;
    offset = other.offset;
    // mark our buffer as shared
    _isShared = true;
    // and mark source as shared
    other._isShared = true;
  }

  /**
   * Create a Buffer using the byte array as the initial value.
   * 
   * @param bytes
   *          This array becomes the backing storage for the object.
   */
  public FlexBuffer(byte[] bytes) {
    set(bytes);
  }

  /**
   * Create a Buffer using the byte range as the initial value.
   * 
   * @param bytes
   *          this array becomes the backing storage for the object.
   * @param offset
   *          offset into byte array
   * @param length
   *          length of data
   */
  public FlexBuffer(byte[] bytes, int offset, int length) {
    set(bytes, offset, length);
  }

  /**
   * Use the specified bytes array as underlying sequence.
   * 
   * @param bytes
   *          byte sequence
   */
  public void set(byte[] bytes) {
    this.count = (bytes == null) ? 0 : bytes.length;
    this.zbytes = bytes;
    this.offset = 0;
    this._isShared = true;
  }

  /**
   * Use the specified bytes array as underlying sequence.
   * 
   */
  public void set(byte[] bytes, int offset, int length) {
    this.count = length;
    this.zbytes = bytes;
    this.offset = offset;
    this._isShared = true;
  }

  /**
   * Use the specified bytes array as underlying sequence.
   * 
   */
  public void set(byte[] bytes, int offset, int length, boolean shared) {
    this.count = length;
    this.zbytes = bytes;
    this.offset = offset;
    this._isShared = shared;
  }

  /**
   * Copy the specified byte array to the Buffer. Replaces the current buffer.
   * 
   * @param bytes
   *          byte array to be assigned
   * @param offset
   *          offset into byte array
   * @param length
   *          length of data
   */
  public final void copy(byte[] bytes, int offset, int length) {
    if (this.zbytes == null || (this.zbytes.length - offset) < length) {
      // alloc a fresh buffer
      this.zbytes = new byte[length];
      // reset offset
      this.offset = 0;
      // reset shared flag
      this._isShared = false;
    }
    if (this._isShared) {
      copyOnWrite();
    }
    System.arraycopy(bytes, offset, this.zbytes, this.offset, length);
    this.count = length;
  }

  /**
   * Get the data from the Buffer.
   * 
   * @return The data is only valid between 0 and getCount() - 1.
   */
  public byte[] get() {
    if (zbytes == null) {
      zbytes = new byte[0];
      offset = 0;
    }
    return zbytes;
  }

  /**
   * Get the current count of the buffer.
   */
  public int getCount() {
    return count;
  }

  /**
   * Set Count
   */
  public void setCount(int count) {
    if (getCapacity() < count) {
      setCapacity(count);
    }
    this.count = count;
  }

  /** Get current offset **/
  public int getOffset() {
    return offset;
  }

  /**
   * Get the capacity, which is the maximum count that could handled without
   * resizing the backing storage.
   * 
   * @return The number of bytes
   */
  public int getCapacity() {
    return this.get().length - offset;
  }

  /**
   * Change the capacity of the backing storage. The data is preserved if
   * newCapacity >= getCount().
   * 
   * @param newCapacity
   *          The new capacity in bytes.
   */
  public void setCapacity(int newCapacity) {
    if (newCapacity < 0) {
      throw new IllegalArgumentException("Invalid capacity argument "
          + newCapacity);
    }
    if (newCapacity == 0) {
      this.zbytes = null;
      this.count = 0;
      this.offset = 0;
      this._isShared = false;
      return;
    }
    if (newCapacity != getCapacity()) {
      byte[] data = new byte[newCapacity];
      if (newCapacity < count) {
        count = newCapacity;
      }
      if (count != 0) {
        System.arraycopy(this.get(), offset, data, 0, count);
      }
      zbytes = data;
      offset = 0;
      _isShared = false;
    } else {
      copyOnWrite();
    }
  }

  /**
   * Reset the buffer to 0 size
   */
  public void reset() {
    setCapacity(0);
  }

  /**
   * Change the capacity of the backing store to be the same as the current
   * count of buffer.
   */
  public void truncate() {
    setCapacity(count);
  }

  /**
   * Append specified bytes to the buffer.
   * 
   * @param bytes
   *          byte array to be appended
   * @param offset
   *          offset into byte array
   * @param length
   *          length of data
   */
  public void append(byte[] bytes, int offset, int length) {
    setCapacity(count + length);
    System.arraycopy(bytes, offset, this.get(), offset + count, length);
    count = count + length;
  }

  /**
   * Append specified bytes to the buffer
   * 
   * @param bytes
   *          byte array to be appended
   */
  public void append(byte[] bytes) {
    append(bytes, 0, bytes.length);
  }

  // inherit javadoc
  public int hashCode() {
    int hash = 1;
    byte[] b = this.get();
    for (int i = 0; i < count; i++)
      hash = (31 * hash) + (int) b[offset + i];
    return hash;
  }

  /**
   * Define the sort order of the Buffer.
   * 
   * @param other
   *          The other buffer
   * @return Positive if this is bigger than other, 0 if they are equal, and
   *         negative if this is smaller than other.
   */
  public int compareTo(Object other) {
    FlexBuffer right = ((FlexBuffer) other);
    byte[] lb = this.get();
    int leftOffset = this.getOffset();
    byte[] rb = right.get();
    int rightOffset = right.getOffset();
    for (int i = 0; i < count && i < right.count; i++) {
      int a = (lb[i + leftOffset] & 0xff);
      int b = (rb[i + rightOffset] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return count - right.count;
  }

  // inherit javadoc
  public boolean equals(Object other) {
    if (other instanceof FlexBuffer && this != other) {
      return compareTo(other) == 0;
    }
    return (this == other);
  }

  // inheric javadoc
  public String toString() {
    StringBuffer sb = new StringBuffer(2 * count);
    for (int idx = offset; idx < count + offset; idx++) {
      sb.append(Character.forDigit((zbytes[idx] & 0xF0) >> 4, 16));
      sb.append(Character.forDigit(zbytes[idx] & 0x0F, 16));
    }
    return sb.toString();
  }

  /**
   * Convert the byte buffer to a string an specific character encoding
   * 
   * @param charsetName
   *          Valid Java Character Set Name
   */
  public String toString(String charsetName)
      throws UnsupportedEncodingException {
    return new String(this.get(), offset, this.getCount(), charsetName);
  }

  // inherit javadoc
  public Object clone() throws CloneNotSupportedException {
    FlexBuffer result = (FlexBuffer) super.clone();
    result.copy(this.get(), offset, this.getCount());
    return result;
  }

  /**
   * Allocate a new backing store if existing one is shared
   * 
   */
  public void copyOnWrite() {
    if (_isShared) {
      if (zbytes != null) {
        int actualSize = zbytes.length - offset;
        byte[] data = null;
        if (actualSize != 0) {
          data = new byte[actualSize];
          if (count != 0) {
            System.arraycopy(zbytes, offset, data, 0, count);
          }
        }
        zbytes = data;
        offset = 0;
      }
      _isShared = false;
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int byteCount = WritableUtils.readVInt(in);
    setCount(byteCount);
    if (byteCount != 0) {
      // allocate new backing store if shared
      copyOnWrite();
      // read into the array
      in.readFully(get(), getOffset(), byteCount);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write out count
    WritableUtils.writeVInt(out, getCount());
    // and bytes if count is not zero ...
    if (getCount() != 0) {
      out.write(get(), getOffset(), getCount());
    }
  }
}
