package org.commoncrawl.util.shared;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSInputStream;

/**
 * Helper class that wraps a ByteBuffer as an InputStream
 * 
 * @author rana
 * 
 */
public class ByteBufferInputStream extends FSInputStream {

  ByteBuffer _source;

  public ByteBufferInputStream() {
    _source = null;
  }

  public ByteBufferInputStream(ByteBuffer source) {
    _source = source;
  }
  
  public void setSoruce(ByteBuffer source) { 
    _source = source;
  }

  @Override
  public synchronized int read() throws IOException {
    if (_source == null || !_source.hasRemaining()) {
      return -1;
    }
    return _source.get() & 0xff;
  }

  @Override
  public synchronized int read(byte[] bytes, int off, int len)
      throws IOException {
    if (_source == null) {  
      return -1;
    }
    else { 
      // Read only what's left
      if (len == 0)
        return 0;
      else if (_source.remaining() == 0) 
        return -1;
      else {
        len = Math.min(len, _source.remaining());
        _source.get(bytes, off, len);
        return len;
      }
    }
  }

  @Override
  public long getPos() throws IOException {
    return _source.position();
  }

  @Override
  public void seek(long pos) throws IOException {
    _source.position((int)pos);
    
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    seek(targetPos);
    return false;
  }
}
