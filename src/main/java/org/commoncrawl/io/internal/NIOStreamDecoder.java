/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.io.internal;

/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Sun designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Sun in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Sun Microsystems, Inc., 4150 Network Circle, Santa Clara,
 * CA 95054 USA or visit www.sun.com if you need additional information or
 * have any questions.
 */

/*
 * @(#)NIOStreamDecoder.java 1.24 07/05/05
 */

/**
 * MODIFIED TO UTILIZE NIOBuffer more efficiently
 * 
 *  @author rana
 *  
 */
/*Derrived FROM Decoder in Java Source */


import java.io.*;
import java.nio.*;
import java.nio.charset.*;

public class NIOStreamDecoder extends Reader
{


    private volatile boolean isOpen = true;

    private void ensureOpen() throws IOException {
  if (!isOpen)
      throw new IOException("Stream closed");
    }

    // In order to handle surrogates properly we must never try to produce
    // fewer than two characters at a time.  If we're only asked to return one
    // character then the other is saved here to be returned later.
    //
    private boolean haveLeftoverChar = false;
    private char leftoverChar;


    // -- Public methods corresponding to those in InputStreamReader --

    // All synchronization and state/argument checking is done in these public
    // methods; the concrete stream-decoder subclasses defined below need not
    // do any such checking.

    public String getEncoding() {
  if (isOpen())
      return encodingName();
  return null;
    }

    public int read() throws IOException {
        return read0();
    }

    private int read0() throws IOException {
  synchronized (lock) {

      // Return the leftover char, if there is one
      if (haveLeftoverChar) {
    haveLeftoverChar = false;
    return leftoverChar;
      }

      // Convert more bytes
      char cb[] = new char[2];
      int n = read(cb, 0, 2);
      switch (n) {
      case -1:
    return -1;
      case 2:
    leftoverChar = cb[1];
    haveLeftoverChar = true;
    // FALL THROUGH
      case 1:
    return cb[0];
      default:
    assert false : n;
    return -1;
      }
  }
    }

    public int read(char cbuf[], int offset, int length) throws IOException {
  int off = offset;
  int len = length;
        synchronized (lock) {
            ensureOpen();
            if ((off < 0) || (off > cbuf.length) || (len < 0) ||
                ((off + len) > cbuf.length) || ((off + len) < 0)) {
                throw new IndexOutOfBoundsException();
            }
      if (len == 0)
                return 0;

      int n = 0;

      if (haveLeftoverChar) {
    // Copy the leftover char into the buffer
    cbuf[off] = leftoverChar;
    off++; len--;
    haveLeftoverChar = false;
    n = 1;
    if ((len == 0) || !implReady())
        // Return now if this is all we can produce w/o blocking
        return n;
      }

      if (len == 1) {
    // Treat single-character array reads just like read()
    int c = read0();
    if (c == -1)
        return (n == 0) ? -1 : n;
    cbuf[off] = (char)c;
    return n + 1;
      }

      return n + implRead(cbuf, off, off + len);
        }
    }

    public boolean ready() throws IOException {
        synchronized (lock) {
            ensureOpen();
      return haveLeftoverChar || implReady();
        }
    }

    public void close() throws IOException {
        synchronized (lock) {
            if (!isOpen)
                return;
      implClose();
      isOpen = false;
  }
    }

    private boolean isOpen() {
  return isOpen;
    }


    // -- Charset-based stream decoder impl --


    private Charset cs;
    private CharsetDecoder decoder;
    private ByteBuffer bb;

    // Exactly one of these is non-null
    private NIOBufferList ch;


    NIOStreamDecoder(NIOBufferList ch, CharsetDecoder dec) throws IOException {
        this.ch = ch;
        this.decoder = dec;
        this.cs = dec.charset();
        this.bb = ch.read();
    }

    private int readBytes() throws IOException {
            
      try {
        // put back existing buffer if there is still usable data in it ...   
        if (bb.remaining() != 0)
          ch.putBack(bb);
        bb = null;
        // now read from the buffer (with a min size of 8)...
        bb = ch.read(8);
      }
      catch (IOException e) {
        bb = null;
      }

      if (bb != null)
        return bb.remaining();
      else 
        return -1;
    }

    int implRead(char[] cbuf, int off, int end) throws IOException {

        // In order to handle surrogate pairs, this method requires that
        // the invoker attempt to read at least two characters.  Saving the
        // extra character, if any, at a higher level is easier than trying
        // to deal with it here.
        assert (end - off > 1);

        CharBuffer cb = CharBuffer.wrap(cbuf, off, end - off);
        if (cb.position() != 0)
      // Ensure that cb[0] == cbuf[off]
      cb = cb.slice();

        boolean eof = false;
        for (;;) {
      CoderResult cr = decoder.decode(bb, cb, eof);
      if (cr.isUnderflow()) {
          if (eof)
        break;
          if (!cb.hasRemaining())
        break;
          if ((cb.position() > 0) && !inReady())
        break;    // Block at most once
          int n = readBytes();
          if (n < 0) {
        eof = true;
        if ((cb.position() == 0) && (bb == null || !bb.hasRemaining()))
            break;
        decoder.reset();
          }
          continue;
      }
      if (cr.isOverflow()) {
          assert cb.position() > 0;
          break;
      }
      cr.throwException();
        }

        if (eof) {
      // ## Need to flush decoder
      decoder.reset();
        }

        if (cb.position() == 0) {
            if (eof)
              return -1;
            assert false;
        }
        return cb.position();
    }

    String encodingName() {
        return (cs.name());
    }

    private boolean inReady() {
      return ((ch != null) && (ch.isDataAvailable()));
    }

    boolean implReady() {
            return bb.hasRemaining() || inReady();
    }

    void implClose() throws IOException {
      if (bb != null)
        bb = null;
    }

}