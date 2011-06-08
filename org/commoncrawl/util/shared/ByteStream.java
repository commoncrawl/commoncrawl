package org.commoncrawl.util.shared;

import java.io.ByteArrayOutputStream;

/**
 * Helper class to gain access to ByteArrayOutputStream's underlying storage
 * 
 * @author rana
 * 
 */
public final class ByteStream extends ByteArrayOutputStream {
  public ByteStream(int initialSize) {
    super(initialSize);
  }

  public byte[] getBuffer() {
    return buf;
  }
}