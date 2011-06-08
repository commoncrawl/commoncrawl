package org.commoncrawl.util.shared;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.record.Buffer;

/**
 * Helper class that wraps a ByteBuffer as an OutputStream
 * 
 * @author rana
 * 
 */
public class ByteBufferOutputStream extends OutputStream {
  public Buffer _buffer = new Buffer();
  public byte[] _byte   = new byte[1];

  @Override
  public void write(int b) throws IOException {
    _byte[0] = (byte) b;
    write(_byte, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (_buffer.getCapacity() < _buffer.getCount() + len) {
      _buffer.setCapacity(Math.max(_buffer.getCapacity() << 1, _buffer
          .getCount()
          + len));
    }
    _buffer.append(b, off, len);
  }
}
