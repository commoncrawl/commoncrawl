package org.commoncrawl.util.shared;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Helper class that wraps a ByteBuffer as an InputStream
 * 
 * @author rana
 * 
 */
public class ByteBufferInputStream extends InputStream {

  ByteBuffer _source;

  public ByteBufferInputStream(ByteBuffer source) {
    _source = source;
  }

  @Override
  public synchronized int read() throws IOException {
    if (!_source.hasRemaining()) {
      return -1;
    }
    return _source.get() & 0xff;
  }

  @Override
  public synchronized int read(byte[] bytes, int off, int len)
      throws IOException {
    // Read only what's left
    len = Math.min(len, _source.remaining());
    _source.get(bytes, off, len);
    return len;
  }
}
