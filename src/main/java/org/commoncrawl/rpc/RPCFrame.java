package org.commoncrawl.rpc;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.io.internal.NIOBufferList;
import org.commoncrawl.io.internal.NIOBufferListInputStream;
import org.commoncrawl.io.internal.NIOBufferListOutputStream;
import org.commoncrawl.rpc.UnitTestStruct1;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.TextBytes;
import org.junit.Test;

/**
 * The Framing Protocol used to represent a wire level CC Message
 * 
 * @author rana
 *
 */
public class RPCFrame {

  // FRAME:
  // 2 Bytes - SIGNATURE
  // 2 Bytes - VERSION
  // 4 Bytes - MSG HEADER SIZE
  // 4 Bytes - PAYLOAD SIZE

  public static final Log LOG                     = LogFactory
                                                      .getLog(RPCFrame.class);

  static int              FRAME_HEADER_SIZE       = 12;
  static byte[]           SIGNATURE               = { 'C', 'C' };
  static byte[]           VERSION                 = { '0', '0' };
  static int              MSG_HEADER_SIZE_OFFSET  = 4;
  static int              MSG_PAYLOAD_SIZE_OFFSET = 8;

  enum MSG_TYPE {

    // a standard request or response message
    // containing a serialized Message object
    REQUEST, RESPONSE, ERROR
  }

  static final int MSG_HEADER_FIELD_TYPE          = 1;
  static final int MSG_HEADER_FIELD_SERVICE       = 2;
  static final int MSG_HEADER_FIELD_METHOD        = 3;
  static final int MSG_HEADER_FIELD_REQUEST_ID    = 4;
  static final int MSG_HEADER_FIELD_STATUS        = 5;

  static final int REQUEST_ESSENTIAL_FIELDS_MASK  = (1 << MSG_HEADER_FIELD_TYPE)
                                                      | (1 << MSG_HEADER_FIELD_SERVICE)
                                                      | (1 << MSG_HEADER_FIELD_METHOD)
                                                      | (1 << MSG_HEADER_FIELD_REQUEST_ID);

  static final int RESPONSE_ESSENTIAL_FIELDS_MASK = (1 << MSG_HEADER_FIELD_TYPE)
                                                      | (1 << MSG_HEADER_FIELD_REQUEST_ID)
                                                      | (1 << MSG_HEADER_FIELD_STATUS);

  public static class OutgoingFrame {

  }

  private static final class PayloadOutputStream extends ByteArrayOutputStream {

    byte[] getBuffer() {
      return buf;
    }

    int getLength() {
      return count;
    }

  }

  private static final class HeaderOutputStream extends ByteArrayOutputStream {

    public enum FieldType {
      Byte, Short, Integer, UTFString
    }

    public final void writeFiledCount(int fieldCount) throws IOException {
      write((byte) fieldCount);
    }

    public final void writeByteField(int fieldId, int v) throws IOException {

      // write out id
      write((byte) fieldId);
      // write out type ...
      write((byte) FieldType.Byte.ordinal());
      // write out data ...
      write((byte) (v));
    }

    @SuppressWarnings("unused")
    public final void writeShortField(int fieldId, int v) throws IOException {

      // write out id
      write((byte) fieldId);
      // write out type ...
      write((byte) FieldType.Short.ordinal());
      // write out data
      write((byte) (v >>> 8) & 0xFF);
      write((byte) (v >>> 0) & 0xFF);
    }

    public final void writeIntField(int fieldId, int v) throws IOException {
      // write out id
      write((byte) fieldId);
      // write out type ...
      write((byte) FieldType.Integer.ordinal());
      // fixed length for integer types so, no need to encode length ...
      writeInt(v);
    }

    private final void writeInt(int v) {

      // fixed length for integer types so, no need to encode length ...
      write((v >>> 24) & 0xFF);
      write((v >>> 16) & 0xFF);
      write((v >>> 8) & 0xFF);
      write((v >>> 0) & 0xFF);

    }

    public void writeUTFField(int fieldId, String str) throws IOException {

      int strlen = str.length();
      int c;

      // write out id
      write((byte) fieldId);

      // write out type ...
      write((byte) FieldType.UTFString.ordinal());

      // calculate approximate buffer space required ...
      int newcount = this.count + (str.length() * 3) + 4 /* utflength */;
      if (newcount > buf.length) {
        byte newbuf[] = new byte[Math.max(buf.length << 1, newcount)];
        System.arraycopy(buf, 0, newbuf, 0, count);
        buf = newbuf;
      }

      // remember utflength length pos
      int utfLengthPos = count;
      // skip field length amount of bytes
      count += 4;

      // write out string length
      writeInt(str.length());

      // write out actual string data
      int i = 0;
      for (i = 0; i < strlen; i++) {
        c = str.charAt(i);
        if (!((c >= 0x0001) && (c <= 0x007F)))
          break;
        buf[count++] = (byte) c;
      }

      for (; i < strlen; i++) {
        c = str.charAt(i);
        if ((c >= 0x0001) && (c <= 0x007F)) {
          buf[count++] = (byte) c;

        } else if (c > 0x07FF) {
          buf[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
          buf[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
          buf[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
        } else {
          buf[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
          buf[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
        }
      }
      // calculate actual bytes written ...
      int fieldLength = count - (utfLengthPos + 4);
      // and finally, write out field length (at saved location ... )
      buf[utfLengthPos + 0] = (byte) ((fieldLength >>> 24) & 0xFF);
      buf[utfLengthPos + 1] = (byte) ((fieldLength >>> 16) & 0xFF);
      buf[utfLengthPos + 2] = (byte) ((fieldLength >>> 8) & 0xFF);
      buf[utfLengthPos + 3] = (byte) ((fieldLength >>> 0) & 0xFF);

    }

    byte[] getBuffer() {
      return buf;
    }

    int getLength() {
      return count;
    }

  }

  public static final class Encoder {

    private NIOBufferListOutputStream _stream;

    public Encoder(NIOBufferListOutputStream stream) {
      _stream = stream;
    }

    @SuppressWarnings("unchecked")
    public synchronized int encodeResponse(IncomingMessageContext context) throws IOException {

      HeaderOutputStream headerStream = new HeaderOutputStream();

      PayloadOutputStream payload = new PayloadOutputStream();
      DataOutput payloadStream = new DataOutputStream(payload);

      // write out number of fields we are going to write out ...
      headerStream.writeFiledCount(3);

      headerStream.writeByteField(RPCFrame.MSG_HEADER_FIELD_TYPE,
          RPCFrame.MSG_TYPE.RESPONSE.ordinal());

      headerStream.writeIntField(RPCFrame.MSG_HEADER_FIELD_REQUEST_ID, context
          .getRequestId());

      headerStream.writeByteField(RPCFrame.MSG_HEADER_FIELD_STATUS, context
          .getStatus().ordinal());

      if (context.getStatus() != OutgoingMessageContext.Status.Success) {
        if (context.getErrorDesc() != null
            && context.getErrorDesc().length() != 0) {
          payloadStream.writeUTF(context.getErrorDesc());
        }
      } else {
        context.getOutput().serialize(payloadStream, new BinaryProtocol());
      }

      payload.flush();

      return encode(headerStream, payload);

    }

    @SuppressWarnings("unchecked")
    public synchronized int encodeRequest(OutgoingMessageContext request) throws IOException {

      HeaderOutputStream headerStream = new HeaderOutputStream();

      PayloadOutputStream payload = new PayloadOutputStream();
      DataOutput payloadStream = new DataOutputStream(payload);

      // write out number of fields we are going to write out ...
      headerStream.writeFiledCount(4);

      headerStream.writeByteField(RPCFrame.MSG_HEADER_FIELD_TYPE,
          RPCFrame.MSG_TYPE.REQUEST.ordinal());

      headerStream.writeUTFField(RPCFrame.MSG_HEADER_FIELD_SERVICE, request
          .getServiceName());

      headerStream.writeUTFField(RPCFrame.MSG_HEADER_FIELD_METHOD, request
          .getMethodName());

      headerStream.writeIntField(RPCFrame.MSG_HEADER_FIELD_REQUEST_ID, request
          .getRequestId());

      request.getInput().serialize(payloadStream, new BinaryProtocol());

      payload.flush();

      return encode(headerStream, payload);
    }

    private synchronized int encode(HeaderOutputStream header, PayloadOutputStream payload)
        throws IOException {

      DataOutput out = new DataOutputStream(_stream);

      out.write(RPCFrame.SIGNATURE);
      out.write(RPCFrame.VERSION);

      // write header and payload sizes out to the stream
      out.writeInt(header.getLength());
      out.writeInt(payload.getLength());
      // and finally write out data
      out.write(header.getBuffer(), 0, header.getLength());
      out.write(payload.getBuffer(), 0, payload.getLength());

      _stream.flush();

      return RPCFrame.FRAME_HEADER_SIZE + header.getLength()
          + payload.getLength();
    }

    public void reset() {

    }
  }

  public static final class PayloadInputStream extends FilterInputStream {

    private int _payloadBytes;

    public PayloadInputStream(InputStream in, int payloadSize) {
      super(in);
      _payloadBytes = payloadSize;
    }

    // @Override
    public int available() throws IOException {
      return _payloadBytes;
    }

    // @Override
    public int read() throws IOException {
      if (_payloadBytes == 0) {
        throw new IOException("Out of Bounds Read");
      }
      _payloadBytes--;

      return super.read();

    }

    // @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (_payloadBytes < len) {
        throw new IOException("Out of Bounds Read");
      }
      _payloadBytes -= len;
      return super.read(b, off, len);
    }

    // @Override
    public int read(byte[] b) throws IOException {
      if (_payloadBytes < b.length) {
        throw new IOException("Out of Bounds Read");
      }
      _payloadBytes -= b.length;
      return super.read(b);
    }

    // @Override
    public long skip(long n) throws IOException {
      if (_payloadBytes < n) {
        throw new IOException("Out of Bounds Read");
      }
      _payloadBytes -= n;

      return super.skip(n);
    }

  };

  public static final class IncomingFrame {

    public int                _type;
    public int                _requestId;
    public int                _status;

    public String             _service;
    public String             _method;
    public PayloadInputStream _payload;
  };

  static final class HeaderInputStream {

    private byte[] bytearr;
    private int    count = 0;

    public HeaderInputStream(byte[] data) {
      bytearr = data;
    }

    public final void checkBounds(int bytesRequired) throws IOException {
      if (count + bytesRequired > bytearr.length)
        throw new EOFException();
    }

    public final void skip(int bytes) throws IOException {
      checkBounds(bytes);
      count += bytes;
    }

    public final byte readByte() throws IOException {

      checkBounds(1);

      int ch = (int) (bytearr[count++] & 0xff);

      if (ch < 0)
        throw new EOFException();

      return (byte) (ch);
    }

    public final short readShort() throws IOException {

      checkBounds(2);

      int ch1 = (int) (bytearr[count++] & 0xff);
      int ch2 = (int) (bytearr[count++] & 0xff);

      if ((ch1 | ch2) < 0)
        throw new EOFException();
      return (short) ((ch1 << 8) + (ch2 << 0));
    }

    public final int readInt() throws IOException {

      checkBounds(4);

      int ch1 = (int) (bytearr[count++] & 0xff);
      int ch2 = (int) (bytearr[count++] & 0xff);
      int ch3 = (int) (bytearr[count++] & 0xff);
      int ch4 = (int) (bytearr[count++] & 0xff);

      if ((ch1 | ch2 | ch3 | ch4) < 0)
        throw new EOFException();
      return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public final String readUTF() throws IOException {

      // read buffer length ...
      int utflen = readInt();
      // read string length ...
      int strlen = readInt();
      // reduce utflen by sizeof (strlen) variable
      utflen -= 4;

      char[] chararr = new char[strlen];

      int c, char2, char3;
      int chararr_count = 0;
      int charCountMax = count + utflen;

      while (count < charCountMax) {
        c = (int) bytearr[count] & 0xff;
        if (c > 127)
          break;
        count++;
        chararr[chararr_count++] = (char) c;
      }

      while (count < charCountMax) {
        c = (int) bytearr[count] & 0xff;
        switch (c >> 4) {
          case 0:
          case 1:
          case 2:
          case 3:
          case 4:
          case 5:
          case 6:
          case 7:
            /* 0xxxxxxx */
            count++;
            chararr[chararr_count++] = (char) c;
            break;
          case 12:
          case 13:
            /* 110x xxxx 10xx xxxx */
            count += 2;
            if (count > charCountMax)
              throw new UTFDataFormatException(
                  "malformed input: partial character at end");
            char2 = (int) bytearr[count - 1];
            if ((char2 & 0xC0) != 0x80)
              throw new UTFDataFormatException("malformed input around byte "
                  + count);
            chararr[chararr_count++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
            break;
          case 14:
            /* 1110 xxxx 10xx xxxx 10xx xxxx */
            count += 3;
            if (count > charCountMax)
              throw new UTFDataFormatException(
                  "malformed input: partial character at end");
            char2 = (int) bytearr[count - 2];
            char3 = (int) bytearr[count - 1];
            if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
              throw new UTFDataFormatException("malformed input around byte "
                  + (count - 1));
            chararr[chararr_count++] = (char) (((c & 0x0F) << 12)
                | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0));
            break;
          default:
            /* 10xx xxxx, 1111 xxxx */
            throw new UTFDataFormatException("malformed input around byte "
                + count);
        }
      }
      // The number of chars produced may be less than utflen
      return new String(chararr, 0, chararr_count);
    }
  }

  public static final class Decoder {

    NIOBufferListInputStream _stream;
    byte[]                   _lookAheadBuffer = new byte[FRAME_HEADER_SIZE];
    int                      _lookAheadSize   = 0;
    int                      _headerSize      = -1;
    int                      _payloadSize     = -1;

    Decoder(NIOBufferListInputStream stream) {
      _stream = stream;
    }

    public void reset() {
      _lookAheadSize = 0;
      _headerSize = -1;
      _payloadSize = -1;
    }

    private static int readInt(byte[] fromArray, int offset) {
      int ch1 = (int) (fromArray[offset] & 0xff);
      int ch2 = (int) (fromArray[offset + 1] & 0xff);
      int ch3 = (int) (fromArray[offset + 2] & 0xff);
      int ch4 = (int) (fromArray[offset + 3] & 0xff);

      return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
    }

    public IncomingFrame getNextRequestFrame() throws IOException {
      return getNextFrame(true);
    }

    public IncomingFrame getNextResponseFrame() throws IOException {
      return getNextFrame(false);
    }

    private IncomingFrame getNextFrame(boolean isRequestFrame)
        throws IOException {

      IncomingFrame frame = new IncomingFrame();

      try {

        if (_lookAheadSize != FRAME_HEADER_SIZE && _stream.available() != 0) {
          // read header bytes ...
          _lookAheadSize += _stream.read(_lookAheadBuffer, _lookAheadSize,
              FRAME_HEADER_SIZE - _lookAheadSize);
        }
        if (_lookAheadSize == FRAME_HEADER_SIZE) {

          if (_headerSize == -1 || _payloadSize == -1) {

            if (_lookAheadBuffer[0] != SIGNATURE[0]
                || _lookAheadBuffer[1] != SIGNATURE[1]
                || _lookAheadBuffer[2] != VERSION[0]
                || _lookAheadBuffer[3] != VERSION[1]) {
              throw new IOException("Invalid Frame Header Detected:"
                  + _lookAheadBuffer.toString());
            }

            _headerSize = readInt(_lookAheadBuffer, MSG_HEADER_SIZE_OFFSET);
            _payloadSize = readInt(_lookAheadBuffer, MSG_PAYLOAD_SIZE_OFFSET);
          }

          // if there is enough data in the input stream to read the entire msg
          // (header + payload)
          if (_stream.available() >= _headerSize + _payloadSize) {

            // decode the header

            // for safety, read header into separate byte buffer
            byte[] header = new byte[_headerSize];
            _stream.read(header);
            // next read header fields
            HeaderInputStream headerDataStream = new HeaderInputStream(header);
            // read the field count
            int fieldCount = headerDataStream.readByte();

            int fieldMask = 0;

            while (fieldCount-- != 0) {

              // read field id
              int fieldId = headerDataStream.readByte();
              // read field type
              HeaderOutputStream.FieldType fieldType = HeaderOutputStream.FieldType
                  .values()[headerDataStream.readByte()];

              // update field mask
              fieldMask |= (1 << fieldId);

              // do something base on field id
              switch (fieldId) {

                case MSG_HEADER_FIELD_SERVICE: {

                  frame._service = headerDataStream.readUTF();
                }
                  break;

                case MSG_HEADER_FIELD_METHOD: {

                  frame._method = headerDataStream.readUTF();
                }
                  break;

                case MSG_HEADER_FIELD_REQUEST_ID: {

                  frame._requestId = headerDataStream.readInt();
                }
                  break;

                case MSG_HEADER_FIELD_STATUS: {

                  frame._status = headerDataStream.readByte();
                }
                  break;

                case MSG_HEADER_FIELD_TYPE: {

                  frame._type = headerDataStream.readByte();
                }
                  break;

                default: {

                  int skipBytes = 0;

                  switch (fieldType) {

                    case Byte: {
                      skipBytes = 1;
                    }
                      break;

                    case Short: {
                      skipBytes = 2;
                    }
                      break;

                    case Integer: {
                      skipBytes = 4;
                    }
                      break;

                    case UTFString: {
                      skipBytes = headerDataStream.readInt();
                    }
                      break;
                  }

                  headerDataStream.skip(skipBytes);
                }

              }
            }

            int requiredFieldMask = (isRequestFrame == true) ? REQUEST_ESSENTIAL_FIELDS_MASK
                : RESPONSE_ESSENTIAL_FIELDS_MASK;

            if ((fieldMask & requiredFieldMask) != requiredFieldMask) {
              throw new IOException("Invalid Frame Header Detected");
            }

            // otherwise good to go ... create payload stream and return
            // populate frame
            frame._payload = new PayloadInputStream(_stream.subStream(_payloadSize), _payloadSize);

            // and reset state ..
            reset();

            return frame;
          }
        }
        return null;
      } catch (IOException e) {

        StringBuilder debugBuffer = new StringBuilder();

        debugBuffer.append("LookAheadSize == FRAME_HEADER_SIZE:"
            + (_lookAheadSize == FRAME_HEADER_SIZE) + "\n");
        if (_lookAheadSize == FRAME_HEADER_SIZE) {
          if (_lookAheadBuffer[0] != SIGNATURE[0]
              || _lookAheadBuffer[1] != SIGNATURE[1]
              || _lookAheadBuffer[2] != VERSION[0]
              || _lookAheadBuffer[3] != VERSION[1]) {
            debugBuffer.append("Frame Signature is Wrong:\n");
            debugBuffer.append("  Got:[" + _lookAheadBuffer[0] + "]["
                + _lookAheadBuffer[1] + "][" + _lookAheadBuffer[2] + "]["
                + _lookAheadBuffer[3] + "]\n");
            debugBuffer.append("  Exp:[" + SIGNATURE[0] + "][" + SIGNATURE[1]
                + "][" + VERSION[0] + "][" + VERSION[1] + "]\n");
          } else {
            debugBuffer.append("Frame Signature is Valid\n");
            debugBuffer.append("Header Size is:" + _headerSize + "\n");
            debugBuffer.append("Payload Size is:" + _payloadSize + "\n");
            debugBuffer.append("Service Field:" + frame._service + "\n");
            debugBuffer.append("Method  Field:" + frame._method + "\n");
            debugBuffer.append("ReqId   Field:" + frame._requestId + "\n");
            debugBuffer.append("Status  Field:" + frame._status + "\n");
            debugBuffer.append("Type    Field:" + frame._type + "\n");

          }
          LOG.error("getNextFrame Threw Exception:"
              + CCStringUtils.stringifyException(e));
          LOG.error("Details:");
          LOG.error(debugBuffer);
        }
        throw e;
      }

    }

  }

  @Test
  public void testEncoderDecoder() throws Exception {

    NIOBufferList output = new NIOBufferList();
    NIOBufferList input = new NIOBufferList();

    NIOBufferListOutputStream outputStream = new NIOBufferListOutputStream(
        output);
    NIOBufferListInputStream inputStream = new NIOBufferListInputStream(input);

    RPCFrame.Encoder encoder = new RPCFrame.Encoder(outputStream);
    RPCFrame.Decoder decoder = new RPCFrame.Decoder(inputStream);

    /**
     * UnitTestStruct1 based on rpc defiinition: module org.commoncrawl.rpc {
     * 
     * class UnitTestStruct1 {
     * 
     * enum EnumeratedValue { ONE = 1; TWO = 2; }
     * 
     * int intType = 1; long longType = 2; ustring stringType = 3;
     * vector<ustring> vectorOfStrings = 4; }
     * 
     * }
     * */
    UnitTestStruct1 inputStruct = new UnitTestStruct1();
    UnitTestStruct1 outputStruct = new UnitTestStruct1();

    inputStruct.setIntType(10);
    inputStruct.setLongType(20);
    inputStruct.setStringType("one");
    inputStruct.setFieldDirty(UnitTestStruct1.Field_VECTOROFSTRINGS);
    inputStruct.getVectorOfStrings().add(new TextBytes("one"));
    inputStruct.getVectorOfStrings().add(new TextBytes("two"));
    inputStruct.getVectorOfStrings().add(new TextBytes("three"));

    OutgoingMessageContext<UnitTestStruct1, UnitTestStruct1> request = new OutgoingMessageContext<UnitTestStruct1, UnitTestStruct1>(
        "testService", "testMethod", inputStruct, outputStruct, null);

    request.setRequestId(10);
    
    encoder.encodeRequest(request);

    // stream the data in one byte at a time
    while (output.available() != 0) {

      ByteBuffer buffer = output.read();

      input.getWriteBuf().put(buffer.get());
      input.flush();
      if (buffer.remaining() != 0) {
        output.putBack(buffer);
      }
      // validate that the decoder doesn't return the frame until the
      // appropriate time
      if (output.available() != 0)
        assertTrue(decoder.getNextRequestFrame() == null);
    }

    RPCFrame.IncomingFrame incomingFrame = decoder.getNextRequestFrame();
    // at this point the frame should be available ...
    assertTrue(incomingFrame != null);

    // check frame header values ...
    assertTrue(incomingFrame._service.equals("testService"));
    assertTrue(incomingFrame._method.equals("testMethod"));
    assertTrue(incomingFrame._requestId == 10);
    assertTrue(incomingFrame._type == RPCFrame.MSG_TYPE.REQUEST.ordinal());

    // deserialize the input struct
    outputStruct.deserialize(new DataInputStream(incomingFrame._payload),
        new BinaryProtocol());

    // validate values of deserialized struct against original (input) struct

    assertTrue(outputStruct.getIntType() == inputStruct.getIntType());
    assertTrue(outputStruct.getLongType() == inputStruct.getLongType());
    assertTrue(outputStruct.getStringType().equals(inputStruct.getStringType()));
    assertTrue(outputStruct.getVectorOfStrings().size() == inputStruct
        .getVectorOfStrings().size());
    assertTrue(outputStruct.getVectorOfStrings().get(0).equals(
        inputStruct.getVectorOfStrings().get(0)));
    assertTrue(outputStruct.getVectorOfStrings().get(1).equals(
        inputStruct.getVectorOfStrings().get(1)));
    assertTrue(outputStruct.getVectorOfStrings().get(2).equals(
        inputStruct.getVectorOfStrings().get(2)));

  }
}
