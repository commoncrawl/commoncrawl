package org.commoncrawl.util.shared;

import java.io.IOException;
import java.io.InputStream;

/** 
 * Track position of source input stream 
 * 
 * @author rana
 *
 */
class CountingInputStream extends InputStream {

  InputStream source;
  long position;
  
  public CountingInputStream(InputStream source) { 
    this.source = source; 
  }
  
  public long getPosition() { 
    return position;
  }
  
  @Override
  public int read() throws IOException {
    int r = source.read();
    if (r > 0) {
      ++position;
    }
    return r;
  }
  
  @Override
  public int read(byte b[]) throws IOException {
    int r = source.read(b);
    if (r > 0) {
      position += r;
    }
    return r;
  }
  
  @Override
  public int read(byte b[], int off, int len) throws IOException {
    int r = source.read(b, off, len);
    if (r > 0) {
      position += r;
    }
    return r;
  }
  
  @Override
  public long skip(long n) throws IOException {
    long r = source.skip(n);
    if (r > 0) {
      position += r;
    }
    return r;
  }
  
  @Override
  public void close() throws IOException {
    source.close();
  }

}