package org.commoncrawl.util.shared;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;


/**
 * BoundedRangeFIleInputStream abstracts a contiguous region of a Hadoop
 * FSDataInputStream as a regular input stream. One can create multiple
 * BoundedRangeFileInputStream on top of the same FSDataInputStream and they
 * would not interfere with each other.
 * Copied from hadoop-335 tfile.
 */
public class BoundedRangeFileInputStream  extends InputStream implements  Seekable, PositionedReadable {

  static final Log LOG = LogFactory.getLog(BoundedRangeFileInputStream.class);

  private FSDataInputStream in;
  private long pos;
  private long end;
  private long mark;
  private final byte[] oneByte = new byte[1];
  private final boolean pread;

  /**
   * Constructor
   *
   * @param in
   *          The FSDataInputStream we connect to.
   * @param offset
   *          Beginning offset of the region.
   * @param length
   *          Length of the region.
   * @param pread If true, use Filesystem positional read rather than seek+read.
   *
   *          The actual length of the region may be smaller if (off_begin +
   *          length) goes beyond the end of FS input stream.
   */
  public BoundedRangeFileInputStream(FSDataInputStream in, long offset,
      long length) {
    if (offset < 0 || length < 0) {
      throw new IndexOutOfBoundsException("Invalid offset/length: " + offset
          + "/" + length);
    }

    this.in = in;
    this.pos = offset;
    this.end = offset + length;
    this.mark = -1;
    this.pread = true;
  }

  @Override
  public int available() throws IOException {
    int avail = in.available();
    if (pos + avail > end) {
      avail = (int) (end - pos);
    }

    return avail;
  }

  @Override
  public int read() throws IOException {
    int ret = read(oneByte);
    if (ret == 1) return oneByte[0] & 0xff;
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    }

    int n = (int) Math.min(Integer.MAX_VALUE, Math.min(len, (end - pos)));
    if (n == 0) return -1;
    int ret = 0;
    if (this.pread) {
      LOG.info("PREAD Reading at Pos:" + pos + " Bytes:" + n);
      ret = in.read(pos, b, off, n);
    } else {
      synchronized (in) {
        LOG.info("NONPREAD Reading at Pos:" + pos + " Bytes:" + n);
        in.seek(pos);
        ret = in.read(b, off, n);
      }
    }
    if (ret < 0) {
      end = pos;
      return -1;
    }
    pos += ret;
    return ret;
  }

  @Override
  /*
   * We may skip beyond the end of the file.
   */
  public long skip(long n) throws IOException {
    long len = Math.min(n, end - pos);
    pos += len;
    return len;
  }

  @Override
  public void mark(int readlimit) {
    mark = pos;
  }

  @Override
  public void reset() throws IOException {
    if (mark < 0) throw new IOException("Resetting to invalid mark");
    pos = mark;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void close() {
    // Invalidate the state of the stream.
    in = null;
    pos = end;
    mark = -1;
  }

  @Override
  public long getPos() throws IOException {
    return pos;
  }

  @Override
  public void seek(long pos) throws IOException {
    this.pos = pos;
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return in.seekToNewSource(targetPos);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    LOG.info("Reading at Pos:" + position + " Bytes:" + length);
    return in.read(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    LOG.info("Reading at Pos:" + position + " Bytes:" + buffer.length);
    in.readFully(position, buffer);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    LOG.info("Reading at Pos:" + position + " Bytes:" + length);
    in.readFully(position, buffer,offset,length);
  }
}