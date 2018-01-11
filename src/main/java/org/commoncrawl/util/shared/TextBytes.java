package org.commoncrawl.util.shared;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * basically a clone of hadoop Text class with FlexBuffer as the backing store
 * and a cacheable String object that is lazilly populated by the call to the
 * toString API.
 * 
 * From Text.java:
 * 
 * This class stores text using standard UTF8 encoding. It provides methods to
 * serialize, deserialize, and compare texts at byte level. The type of length
 * is integer and is serialized using zero-compressed format.
 * <p>
 * In addition, it provides methods for string traversal without converting the
 * byte array to a string.
 * <p>
 * Also includes utilities for serializing/deserialing a string, coding/decoding
 * a string, checking if a byte array contains valid UTF8 code, calculating the
 * length of an encoded string.
 * 
 * @author rana
 * 
 */
public final class TextBytes extends BinaryComparableWithOffset implements
    WritableComparable<BinaryComparable>,Cloneable {

  private static final Log                   LOG             = LogFactory
                                                                 .getLog(TextBytes.class);

  private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = new ThreadLocal<CharsetEncoder>() {
                                                               protected CharsetEncoder initialValue() {
                                                                 return Charset
                                                                     .forName(
                                                                         "UTF-8")
                                                                     .newEncoder()
                                                                     .onMalformedInput(
                                                                         CodingErrorAction.REPORT)
                                                                     .onUnmappableCharacter(
                                                                         CodingErrorAction.REPORT);
                                                               }
                                                             };

  private static ThreadLocal<CharsetDecoder> DECODER_FACTORY = new ThreadLocal<CharsetDecoder>() {
                                                               protected CharsetDecoder initialValue() {
                                                                 return Charset
                                                                     .forName(
                                                                         "UTF-8")
                                                                     .newDecoder()
                                                                     .onMalformedInput(
                                                                         CodingErrorAction.REPORT)
                                                                     .onUnmappableCharacter(
                                                                         CodingErrorAction.REPORT);
                                                               }
                                                             };

  private static final byte[]                EMPTY_BYTES     = new byte[0];
  private static final String                EMPTY_STRING    = new String("");

  private FlexBuffer                         bytes           = new FlexBuffer();
  private String                             cachedUTF8      = null;

  public TextBytes() {

  }

  /**
   * Construct from a string.
   */
  public TextBytes(String string) {
    set(string);
  }

  /** Construct from another textbytes. */
  public TextBytes(TextBytes utf8,boolean shared) {
    bytes = new FlexBuffer(utf8.bytes,shared);
    cachedUTF8 = utf8.cachedUTF8;
  }
  
  /** 
   * construct from a existing FlexBuffer 
   * 
   * @param buffer
   * @param shared
   */
  public TextBytes(FlexBuffer utf8Data,boolean shared) {
    if (shared) {
      bytes = utf8Data;
    }
    else { 
      bytes = new FlexBuffer(utf8Data,false);
    }
    cachedUTF8 = null;
  }

  /** Construct from another text. */
  public TextBytes(Text utf8,boolean shared) {
    set(utf8,shared);
  }

  /**
   * Construct from a byte array.
   */
  public TextBytes(byte[] utf8,boolean shared) {
    set(utf8,shared);
  }
  
  /**
   * Construct from a byte array with offset and length
   * 
   * @param utf8
   * @param offset
   * @param len
   * @param shared
   */
  public TextBytes(byte[] utf8,int offset,int len,boolean shared) { 
    set(utf8,offset,len,shared);
  }

  /**
   * get underlying buffer object
   */
  public FlexBuffer getBuffer() {
    return bytes;
  }

  /**
   * Returns the raw bytes; however, only data up to {@link #getLength()} is
   * valid.
   */
  public byte[] getBytes() {
    return bytes.get();
  }

  /** Returns the number of bytes in the byte array */
  public int getLength() {
    return bytes.getCount();
  }

  /** Get offset (if set) **/
  public int getOffset() {
    return bytes.getOffset();
  }

  public void setLength(int newLength) {
    if (newLength > getCapacity()) {
      setCapacity(newLength, true);
    }
    bytes.setCount(newLength);
    cachedUTF8 = null;
  }

  /**
   * Returns the Unicode Scalar Value (32-bit integer value) for the character
   * at <code>position</code>. Note that this method avoids using the converter
   * or doing String instatiation
   * 
   * @return the Unicode scalar value at position or -1 if the position is
   *         invalid or points to a trailing byte
   */
  public int charAt(int position) {
    if (position > bytes.getCount())
      return -1; // too long
    if (position < 0)
      return -1; // duh.

    ByteBuffer bb = (ByteBuffer) ByteBuffer.wrap(bytes.get(),
        bytes.getOffset(), bytes.getCount()).position(position);
    return bytesToCodePoint(bb.slice());
  }

  public int find(String what) {
    return find(what, 0);
  }

  /**
   * Finds any occurence of <code>what</code> in the backing buffer, starting as
   * position <code>start</code>. The starting position is measured in bytes and
   * the return value is in terms of byte position in the buffer. The backing
   * buffer is not converted to a string for this operation.
   * 
   * @return byte position of the first occurence of the search string in the
   *         UTF-8 buffer or -1 if not found
   */
  public int find(String what, int start) {
    try {
      ByteBuffer src = ByteBuffer.wrap(bytes.get(), bytes.getOffset(), bytes
          .getCount());
      ByteBuffer tgt = encode(what);
      byte b = tgt.get();
      src.position(start);

      while (src.hasRemaining()) {
        if (b == src.get()) { // matching first byte
          src.mark(); // save position in loop
          tgt.mark(); // save position in target
          boolean found = true;
          int pos = src.position() - 1;
          while (tgt.hasRemaining()) {
            if (!src.hasRemaining()) { // src expired first
              tgt.reset();
              src.reset();
              found = false;
              break;
            }
            if (!(tgt.get() == src.get())) {
              tgt.reset();
              src.reset();
              found = false;
              break; // no match
            }
          }
          if (found)
            return pos;
        }
      }
      return -1; // not found
    } catch (CharacterCodingException e) {
      // can't get here
      e.printStackTrace();
      return -1;
    }
  }

  /**
   * Set to contain the contents of a string.
   */
  public void set(String string) {
    try {
      ByteBuffer bb = encode(string, true);
      set(bb.array(), 0, bb.limit(),false);
      cachedUTF8 = string;
      // zbytes = bb.array();
      // length = bb.limit();
      // offset = 0;
    } catch (CharacterCodingException e) {
      throw new RuntimeException("Should not have happened " + e.toString());
    }
  }

  /**
   * Set to a utf8 byte array
   */
  public void set(byte[] utf8,boolean shared) {
    set(utf8, 0, utf8.length,shared);
  }

  /** copy a text. */
  public void set(Text other,boolean shared) {
    set(other.getBytes(), 0 , other.getLength(),shared);
  }

  /** copy a textbytes. */
  public void set(TextBytes other,boolean shared) {
    set(other.getBytes(), other.getOffset(), other.getLength(),shared);
  }

  /**
   * Set the Text to range of bytes
   * 
   * @param utf8
   *          the data to copy from
   * @param start
   *          the first position of the new string
   * @param len
   *          the number of bytes of the new string
   */
  public void set(byte[] utf8, int start, int len,boolean shared) {
    bytes.set(utf8, start, len,shared);
    // reset string cache ...
    cachedUTF8 = null;
  }

  /**
   * Append a range of bytes to the end of the given text
   * 
   * @param utf8
   *          the data to copy from
   * @param start
   *          the first position to append from utf8
   * @param len
   *          the number of bytes to append
   */
  public void append(byte[] utf8, int start, int len) {
    setCapacity(bytes.getCount() + len, true);
    if (bytes.isShared()) {
      bytes.copyOnWrite();
    }
    System.arraycopy(utf8, start, bytes.get(), bytes.getCount(), len);
    bytes.setCount(bytes.getCount() + len);
    cachedUTF8 = null;
  }

  /**
   * Clear the string to empty.
   */
  public void clear() {
    bytes.setCount(0);
    cachedUTF8 = null;
  }

  /*
   * Sets the capacity of this Text object to <em>at least</em> <code>len</code>
   * bytes. If the current buffer is longer, then the capacity and existing
   * content of the buffer are unchanged. If <code>len</code> is larger than the
   * current capacity, the Text object's capacity is increased to match.
   * 
   * @param len the number of bytes we need
   * 
   * @param keepData should the old data be kept
   */
  private void setCapacity(int len, boolean keepData) {
    if (!keepData) {
      // ok turn off copy on write here ...
      // because setCapacity below will take of this ... 
      boolean oldSharedValue = bytes._isShared;
      bytes._isShared = false;
      bytes.setCount(0);
      bytes._isShared = oldSharedValue;
      
    }
    
    bytes.setCapacity(len);
    cachedUTF8 = null;
  }

  private int getCapacity() {
    return bytes.getCapacity();
  }

  /**
   * Convert text back to string
   * 
   * @see java.lang.Object#toString()
   */
  public String toString() {
    if (cachedUTF8 == null) {
      if (bytes.getCount() == 0) {
        cachedUTF8 = EMPTY_STRING;
      } else {
        try {
          cachedUTF8 = decode(bytes.get(), bytes.getOffset(), bytes.getCount());
          return cachedUTF8;
        } catch (CharacterCodingException e) {
          throw new RuntimeException("Should not have happened " + e.toString());
        }
      }
    }
    return cachedUTF8;
  }

  /**
   * deserialize
   */
  public void readFields(DataInput in) throws IOException {
    int newLength = WritableUtils.readVInt(in);
    // ensure capacity
    setCapacity(newLength, false);
    // in case we need to, ensure we have a private copy of the underlaying
    // array
    bytes.copyOnWrite();
    // read into the array
    in.readFully(bytes.get(), bytes.getOffset(), newLength);
    // reset count varaible
    bytes.setCount(newLength);
    // clear cached String pointer
    cachedUTF8 = null;
  }

  /** Skips over one Text in the input. */
  public static void skip(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    WritableUtils.skipFully(in, length);
  }

  /**
   * serialize write this object to out length uses zero-compressed encoding
   * 
   * @see Writable#write(DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, bytes.getCount());
    if (bytes.getCount() != 0) {
      out.write(bytes.get(), bytes.getOffset(), bytes.getCount());
    }
  }

  /** Returns true iff <code>o</code> is a TextBytes with the same contents. */
  public boolean equals(Object o) {
    if (o instanceof TextBytes)
      return super.equals(o);
    return false;
  }

  public int hashCode() {
    int hash = 1;
    int offset = getOffset();
    int length = getLength();
    byte bytesArray[] = bytes.get();
    for (int i = offset; i < offset + length; i++)
      hash = (31 * hash) + (int) bytesArray[i];
    return hash;
  }

  /** A WritableComparator optimized for Text keys. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(Text.class);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int n1 = WritableUtils.decodeVIntSize(b1[s1]);
      int n2 = WritableUtils.decodeVIntSize(b2[s2]);
      return compareBytes(b1, s1 + n1, l1 - n1, b2, s2 + n2, l2 - n2);
    }
  }

  static {
    // register this comparator
    WritableComparator.define(Text.class, new Comparator());
  }

  // / STATIC UTILITIES FROM HERE DOWN
  /**
   * Converts the provided byte array to a String using the UTF-8 encoding. If
   * the input is malformed, replace by a default value.
   */
  public static String decode(byte[] utf8) throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8), true);
  }

  public static String decode(byte[] utf8, int start, int length)
      throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), true);
  }

  /**
   * Converts the provided byte array to a String using the UTF-8 encoding. If
   * <code>replace</code> is true, then malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the method throws a
   * MalformedInputException.
   */
  public static String decode(byte[] utf8, int start, int length,
      boolean replace) throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), replace);
  }

  private static String decode(ByteBuffer utf8, boolean replace)
      throws CharacterCodingException {
    CharsetDecoder decoder = DECODER_FACTORY.get();
    if (replace) {
      decoder.onMalformedInput(java.nio.charset.CodingErrorAction.REPLACE);
      decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    String str = decoder.decode(utf8).toString();
    // set decoder back to its default value: REPORT
    if (replace) {
      decoder.onMalformedInput(CodingErrorAction.REPORT);
      decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return str;
  }

  /**
   * Converts the provided String to bytes using the UTF-8 encoding. If the
   * input is malformed, invalid chars are replaced by a default value.
   * 
   * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
   *         ByteBuffer.limit()
   */

  public static ByteBuffer encode(String string)
      throws CharacterCodingException {
    return encode(string, true);
  }

  /**
   * Converts the provided String to bytes using the UTF-8 encoding. If
   * <code>replace</code> is true, then malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the method throws a
   * MalformedInputException.
   * 
   * @return ByteBuffer: bytes stores at ByteBuffer.array() and length is
   *         ByteBuffer.limit()
   */
  public static ByteBuffer encode(String string, boolean replace)
      throws CharacterCodingException {
    CharsetEncoder encoder = ENCODER_FACTORY.get();
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPLACE);
      encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    ByteBuffer bytes = encoder.encode(CharBuffer.wrap(string.toCharArray()));
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPORT);
      encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return bytes;
  }

  /**
   * Read a UTF8 encoded string from in
   */
  public static String readString(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    return decode(bytes);
  }

  /**
   * Write a UTF8 encoded string to out
   */
  public static int writeString(DataOutput out, String s) throws IOException {
    ByteBuffer bytes = encode(s);
    int length = bytes.limit();
    WritableUtils.writeVInt(out, length);
    out.write(bytes.array(), 0, length);
    return length;
  }

  // //// states for validateUTF8

  private static final int LEAD_BYTE    = 0;

  private static final int TRAIL_BYTE_1 = 1;

  private static final int TRAIL_BYTE   = 2;

  /**
   * Check if a byte array contains valid utf-8
   * 
   * @param utf8
   *          byte array
   * @throws MalformedInputException
   *           if the byte array contains invalid utf-8
   */
  public static void validateUTF8(byte[] utf8) throws MalformedInputException {
    validateUTF8(utf8, 0, utf8.length);
  }

  /**
   * Check to see if a byte array is valid utf-8
   * 
   * @param utf8
   *          the array of bytes
   * @param start
   *          the offset of the first byte in the array
   * @param len
   *          the length of the byte sequence
   * @throws MalformedInputException
   *           if the byte array contains invalid bytes
   */
  public static void validateUTF8(byte[] utf8, int start, int len)
      throws MalformedInputException {
    int count = start;
    int leadByte = 0;
    int length = 0;
    int state = LEAD_BYTE;
    while (count < start + len) {
      int aByte = ((int) utf8[count] & 0xFF);

      switch (state) {
        case LEAD_BYTE:
          leadByte = aByte;
          length = bytesFromUTF8[aByte];

          switch (length) {
            case 0: // check for ASCII
              if (leadByte > 0x7F)
                throw new MalformedInputException(count);
              break;
            case 1:
              if (leadByte < 0xC2 || leadByte > 0xDF)
                throw new MalformedInputException(count);
              state = TRAIL_BYTE_1;
              break;
            case 2:
              if (leadByte < 0xE0 || leadByte > 0xEF)
                throw new MalformedInputException(count);
              state = TRAIL_BYTE_1;
              break;
            case 3:
              if (leadByte < 0xF0 || leadByte > 0xF4)
                throw new MalformedInputException(count);
              state = TRAIL_BYTE_1;
              break;
            default:
              // too long! Longest valid UTF-8 is 4 bytes (lead + three)
              // or if < 0 we got a trail byte in the lead byte position
              throw new MalformedInputException(count);
          } // switch (length)
          break;

        case TRAIL_BYTE_1:
          if (leadByte == 0xF0 && aByte < 0x90)
            throw new MalformedInputException(count);
          if (leadByte == 0xF4 && aByte > 0x8F)
            throw new MalformedInputException(count);
          if (leadByte == 0xE0 && aByte < 0xA0)
            throw new MalformedInputException(count);
          if (leadByte == 0xED && aByte > 0x9F)
            throw new MalformedInputException(count);
          // falls through to regular trail-byte test!!
        case TRAIL_BYTE:
          if (aByte < 0x80 || aByte > 0xBF)
            throw new MalformedInputException(count);
          if (--length == 0) {
            state = LEAD_BYTE;
          } else {
            state = TRAIL_BYTE;
          }
          break;
      } // switch (state)
      count++;
    }
  }

  /**
   * Magic numbers for UTF-8. These are the number of bytes that <em>follow</em>
   * a given lead byte. Trailing bytes have the value -1. The values 4 and 5 are
   * presented in this table, even though valid UTF-8 cannot include the five
   * and six byte sequences.
   */
  static final int[] bytesFromUTF8 = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0,
      0,
      0,
      0,
      0,
      // trail bytes
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
      1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2,
      2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4,
      4, 5, 5, 5, 5               };

  /**
   * Returns the next code point at the current position in the buffer. The
   * buffer's position will be incremented. Any mark set on this buffer will be
   * changed by this method!
   */
  public static int bytesToCodePoint(ByteBuffer bytes) {
    bytes.mark();
    byte b = bytes.get();
    bytes.reset();
    int extraBytesToRead = bytesFromUTF8[(b & 0xFF)];
    if (extraBytesToRead < 0)
      return -1; // trailing byte!
    int ch = 0;

    switch (extraBytesToRead) {
      case 5:
        ch += (bytes.get() & 0xFF);
        ch <<= 6; /* remember, illegal UTF-8 */
      case 4:
        ch += (bytes.get() & 0xFF);
        ch <<= 6; /* remember, illegal UTF-8 */
      case 3:
        ch += (bytes.get() & 0xFF);
        ch <<= 6;
      case 2:
        ch += (bytes.get() & 0xFF);
        ch <<= 6;
      case 1:
        ch += (bytes.get() & 0xFF);
        ch <<= 6;
      case 0:
        ch += (bytes.get() & 0xFF);
    }
    ch -= offsetsFromUTF8[extraBytesToRead];

    return ch;
  }

  static final int offsetsFromUTF8[] = { 0x00000000, 0x00003080, 0x000E2080,
      0x03C82080, 0xFA082080, 0x82082080 };

  /**
   * For the given string, returns the number of UTF-8 bytes required to encode
   * the string.
   * 
   * @param string
   *          text to encode
   * @return number of UTF-8 bytes required to encode
   */
  public static int utf8Length(String string) {
    CharacterIterator iter = new StringCharacterIterator(string);
    char ch = iter.first();
    int size = 0;
    while (ch != CharacterIterator.DONE) {
      if ((ch >= 0xD800) && (ch < 0xDC00)) {
        // surrogate pair?
        char trail = iter.next();
        if ((trail > 0xDBFF) && (trail < 0xE000)) {
          // valid pair
          size += 4;
        } else {
          // invalid pair
          size += 3;
          iter.previous(); // rewind one
        }
      } else if (ch < 0x80) {
        size++;
      } else if (ch < 0x800) {
        size += 2;
      } else {
        // ch < 0x10000, that is, the largest char value
        size += 3;
      }
      ch = iter.next();
    }
    return size;
  }

  public static void main(String[] args) {
    // run some tests on the new code
    String aTestString = new String("A Test Strnig");
    // convert it to bytes
    byte bytes[] = aTestString.getBytes();
    // over allocate an array
    byte overAllocated[] = new byte[bytes.length * 2];
    // copy source
    System.arraycopy(bytes, 0, overAllocated, bytes.length, bytes.length);
    // now allocate a TextBytes
    TextBytes textBytes = new TextBytes();
    // set the overallocated buffer as the backing store
    textBytes.set(overAllocated, bytes.length, bytes.length,false);
    // convert it to string first
    String toString = textBytes.toString();
    // validate equal to original
    Assert.assertTrue(aTestString.equals(toString));
    // ok now write it to output buffer
    DataOutputBuffer outputBuffer = new DataOutputBuffer();
    // write string
    try {
      textBytes.write(outputBuffer);
      // read length
      DataInputBuffer inputBuffer = new DataInputBuffer();
      inputBuffer.reset(outputBuffer.getData(), 0, outputBuffer.size());
      int encodedLength = WritableUtils.readVInt(inputBuffer);
      // validate arrays match ...
      Assert.assertTrue(encodedLength == bytes.length);
      Assert.assertEquals(WritableComparator.compareBytes(bytes, 0,
          bytes.length, outputBuffer.getData(), inputBuffer.getPosition(),
          outputBuffer.getLength() - inputBuffer.getPosition()), 0);
      // ok reset input buffer again ...
      inputBuffer.reset(outputBuffer.getData(), 0, outputBuffer.size());
      // read in fields
      textBytes.readFields(inputBuffer);
      // ok see if we are not using the original backing store ...
      Assert.assertTrue(textBytes.getBytes() != overAllocated);
      // validate buffers match to original
      Assert.assertEquals(WritableComparator.compareBytes(bytes, 0,
          bytes.length, textBytes.getBytes(), textBytes.getOffset(), textBytes
              .getLength()), 0);

    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    TextBytes result = new TextBytes();
    result.bytes.copy(this.bytes.get(), this.bytes.getOffset(), this.bytes.getCount());
    result.cachedUTF8 = this.cachedUTF8;
    return result;
  }
}
