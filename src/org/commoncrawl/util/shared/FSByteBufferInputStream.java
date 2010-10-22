package org.commoncrawl.util.shared;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;


/** ByteBuffer based input stream that also implements Seekable and Positionable,
 *  thus allowing it to emulate FSDataInputStream
 * 
 * @author rana
 *
 */
public class FSByteBufferInputStream extends BufferedInputStream implements Seekable, PositionedReadable {
	
	ByteBuffer _source;
	
  public FSByteBufferInputStream(ByteBuffer in) {
  	super(new ByteBufferInputStream(in),in.limit());
  	_source = in;
  	
  }

  public long getPos() throws IOException {
    return _source.position();
  }

  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    _source.position(_source.position() + (int)n);
    return n;
  }

  public void seek(long pos) throws IOException {
    if( pos<0 ) {
      return;
    }
    // optimize: check if the pos is in the buffer
    long end =_source.position();
    long start = end - count;
    if( pos>=start && pos<end) {
      this.pos = (int)(pos-start);
      return;
    }

    // invalidate buffer
    this.pos = 0;
    this.count = 0;

    _source.position((int)pos);
  }

  public boolean seekToNewSource(long targetPos) throws IOException {
    pos = 0;
    count = 0;
    _source.position((int)targetPos);
    return true;
  }

  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
  	_source.mark();
  	_source.position((int)position);
  	int bytesToRead = Math.min(length,_source.remaining());
    _source.get(buffer, offset,bytesToRead );
    _source.reset();
    return bytesToRead;
  }

  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
  	_source.mark();
  	_source.position((int)position);
    _source.get(buffer, offset,length );
    _source.reset();
  }

  public void readFully(long position, byte[] buffer) throws IOException {
  	_source.mark();
  	_source.position((int)position);
    _source.get(buffer);
    _source.reset();
  }
}