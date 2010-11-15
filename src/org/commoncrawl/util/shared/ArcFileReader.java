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

package org.commoncrawl.util.shared;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.crawl.common.shared.Constants;
import org.commoncrawl.io.shared.NIODataSink;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.junit.Assert;
import org.junit.Test;
import org.apache.hadoop.record.Buffer;
import org.apache.hadoop.util.StringUtils;

/**
 * Decompresses ARC file and returns documents as individual ArcFileItems
 * 
 * @author rana
 * 
 */
public final class ArcFileReader extends InflaterInputStream implements
    NIODataSink {

  // ////////////////////////////////////////////////////////////////////////////////
  // data members
  // ////////////////////////////////////////////////////////////////////////////////

  /** logging **/
  private static final Log LOG = LogFactory.getLog(ArcFileReader.class);

  /** internal ByteBuffer wrapper for queuing byte buffers **/
  private static final class BufferItem {

    public BufferItem(ByteBuffer bufferItem) {
      _buffer = bufferItem;
    }

    public ByteBuffer _buffer;
  };

  /**
   * helper dummy stream used to get around some stupid design decisions in
   * InflaterInputStream
   */
  private static InputStream              _dummyStream              = new InputStream() {

                                                                      @Override
                                                                      public int read()
                                                                          throws IOException {
                                                                        return 0;
                                                                      }

                                                                    };

  /** blocking consumer queue **/
  private LinkedBlockingQueue<BufferItem> _consumerQueue            = null;
  /** 32 bit crc **/
  private CRC32                           _crc                      = new CRC32();
  /** flag indicating that this arc file has a header item **/
  private boolean                         _hasHeaderItem            = true;
  /** the arc file header , when available **/
  private String                          _arcFileHeader            = null;
  /** End Of Stream Indicator **/
  private boolean                         _eosReached               = false;
  /** block size used for various operations **/
  public static final int                 DEFAULT_BLOCK_SIZE        = 32 * 1024;
  /** default buffer queue size **/
  public static final int                 DEFAULT_BUFFER_QUEUE_SIZE = 1024;
  /** default timeout value **/
  public static final int                 DEFAULT_TIMEOUT_VALUE     = -1;

  /** allocation block size **/
  private static int                      _blockSize                = DEFAULT_BLOCK_SIZE;
  /** buffer queue size **/
  private static int                      _bufferQueueSize          = DEFAULT_BUFFER_QUEUE_SIZE;
  /** timeout value for blocking input stream operations **/
  private static int                      _ioTimeoutValue           = DEFAULT_TIMEOUT_VALUE;
  /** stream pos variable **/
  private int                             _streamPos                = 0;

  // ////////////////////////////////////////////////////////////////////////////////
  // public API
  // ////////////////////////////////////////////////////////////////////////////////

  /**
   * Costructs a new ArcFileReader object with specified block size (for
   * allocations)
   */
  public ArcFileReader() {

    super(_dummyStream, new Inflater(true), _blockSize);
    // set up buffer queue ...
    _consumerQueue = new LinkedBlockingQueue<BufferItem>(_bufferQueueSize);
    // setup the proper stream...
    super.in = new PushbackInputStream(new InputStream() {

      ByteBuffer _activeBuffer  = null;
      byte       oneByteArray[] = new byte[1];

      @Override
      public int read() throws IOException {
        if (read(oneByteArray, 0, 1) != -1) {
          return oneByteArray[0] & 0xff;
        }
        return -1;
      }

      @Override
      public int read(byte b[], int off, int len) throws IOException {
        if (_activeBuffer == null || _activeBuffer.remaining() == 0) {
          BufferItem nextItem = null;
          try {
            // when io timeout is not specified, block indefinitely...
            if (_ioTimeoutValue == -1) {
              nextItem = _consumerQueue.take();
            }
            // otherwise wait for specified time on io
            else {
              nextItem = _consumerQueue.poll(_ioTimeoutValue,
                  TimeUnit.MILLISECONDS);

              if (nextItem == null) {
                throw new IOException("IO Timeout waiting for Buffer");
              }
            }

          } catch (InterruptedException e) {
            throw new IOException("Thread Interrupted waiting for Buffer");
          }

          if (nextItem._buffer == null) {
            _eosReached = true;
            // EOF CONDITION ...
            return -1;
          } else {
            _activeBuffer = nextItem._buffer;
          }
        }
        final int sizeAvailable = _activeBuffer.remaining();
        final int readSize = Math.min(sizeAvailable, len);

        _activeBuffer.get(b, off, readSize);

        _streamPos += readSize;

        return readSize;
      }
    }, _blockSize);
  }

  /** set the default block size **/
  public static void setBlockSize(int blockSize) {
    _blockSize = blockSize;
  }

  /** set the buffer queue size **/
  public static void setBufferQueueSize(int bufferQueueSize) {
    _bufferQueueSize = bufferQueueSize;
  }

  /** set the default timeout value for blocking io operations **/
  public static void setIOTimeoutValue(int timeoutInMilliseconds) {
    _ioTimeoutValue = timeoutInMilliseconds;
  }

  /** indicate whether this arc file has a header item **/
  public void setArcFileHasHeaderItemFlag(boolean value) {
    _hasHeaderItem = value;
  }

  /**
   * Reset all interal variables and get the Reader ready to process a new
   * ArcFile
   */
  public void resetState() {
    _arcFileHeader = null;
    _consumerQueue.clear();
    _crc.reset();
    _eosReached = false;
    resetInflater();
  }

  /**
   * Checks to see if additional ArcFileItems can be extracted from the current
   * ARC File Stream This is a BLOCKING CALL - it will block on
   * _consumerQueue.take if no data is available...
   * 
   * @return true if another ArcFileItem can be extracted from the stream.
   * @throws IOException
   *           if an error occurs processing ARC file data
   */
  public boolean hasMoreItems() throws IOException {
    if (_arcFileHeader == null && _hasHeaderItem) {
      readARCHeader();
    }
    return readHeader();
  }

  /**
   * blocking call to retrieve next ArcFileItem contained within an ARC File
   * 
   * @return Fully constructed ArcFileItem
   * @throws IOException
   */
  public void getNextItem(ArcFileItem itemOut) throws IOException {

    // preserve incoming arc file name ...
    String arcFileName = itemOut.getArcFileName();
    // reset item
    itemOut.clear();
    // restore arc file name
    itemOut.setArcFileName(arcFileName);

    // read content
    _crc.reset();
    // and reset inflater
    resetInflater();

    // set the arc file stream positon up front
    itemOut.setArcFilePos(getARCFileStreamPos());

    ArcFileBuilder builder = new ArcFileBuilder(itemOut);

    // read header line buffer
    for (;;) {

      byte scanBuffer[] = new byte[_blockSize];
      ByteBuffer byteBuffer = ByteBuffer.wrap(scanBuffer);

      // read up to scan buffer size of data ...
      int readAmount = read(scanBuffer, 0, scanBuffer.length);

      if (readAmount != -1) {
        // update crc calculation
        _crc.update(scanBuffer, 0, readAmount);
        // and limit byte buffer ...
        byteBuffer.limit(readAmount);
        // and then input data input builder
        builder.inputData(byteBuffer);
      } else {
        // validate crc and header length ...
        readTrailer();

        builder.finish();

        // set the compressed content size ...
        itemOut.setArcFileSize(getARCFileStreamPos() - itemOut.getArcFilePos());

        return;
      }
    }
  }

  /**
   * NIODataSink method - called by implementor when all ARC File data has been
   * exhauseted
   * 
   */
  public void finished() {
    try {
      _consumerQueue.put(new BufferItem(null));
    } catch (InterruptedException e) {
    }
  }

  /**
   * NIODataSink method - called by the implementor to queue up compressed ARC
   * File data for processing
   */
  public void available(ByteBuffer availableReadBuffer) {
    try {
      _consumerQueue.put(new BufferItem(availableReadBuffer));
    } catch (InterruptedException e) {
    }
  }

  // ////////////////////////////////////////////////////////////////////////////////
  // internal helpers
  // ////////////////////////////////////////////////////////////////////////////////

  private void resetInflater() {
    inf.reset();
  }

  private void readARCHeader() throws IOException {

    readHeader();

    byte accumBuffer[] = new byte[4096];

    int accumAmount = 0;
    int readAmt = 0;

    while ((readAmt = this.read(accumBuffer, accumAmount, accumBuffer.length
        - accumAmount)) > 0) {
      accumAmount += readAmt;
      if (accumAmount == accumBuffer.length) {
        throw new IOException("Invalid ARC File Header");
      }
    }

    if (readAmt == 0 || accumAmount == 0) {
      throw new IOException("Invalid ARC File Header");
    } else {
      // calculate header crc ...
      _crc.reset();
      _crc.update(accumBuffer, 0, accumAmount);
      // validate crc and header length ...
      readTrailer();
      // and decode header string ...
      _arcFileHeader = new String(accumBuffer, 0, accumAmount, "ISO-8859-1");
    }
  }

  /**
   * GZIP Code derived from GZIPInputStream code
   */

  // GZIP header magic number.
  private final static int GZIP_MAGIC = 0x8b1f;

  /*
   * File header flags.
   */
  private final static int FHCRC      = 2;     // Header CRC
  private final static int FEXTRA     = 4;     // Extra field
  private final static int FNAME      = 8;     // File name
  private final static int FCOMMENT   = 16;    // File comment

  /*
   * Reads GZIP member header.
   */
  private boolean readHeader() throws IOException {

    if (!_eosReached) {

      CheckedInputStream in = new CheckedInputStream(this.in, _crc);

      _crc.reset();

      try {
        // Check header magic
        if (readUShort(in) != GZIP_MAGIC) {
          throw new IOException("Not in GZIP format");
        }
        // Check compression method
        if (readUByte(in) != 8) {
          throw new IOException("Unsupported compression method");
        }
        // Read flags
        int flg = readUByte(in);
        // Skip MTIME, XFL, and OS fields
        skipBytes(in, 6);
        // Skip optional extra field
        if ((flg & FEXTRA) == FEXTRA) {
          skipBytes(in, readUShort(in));
        }
        // Skip optional file name
        if ((flg & FNAME) == FNAME) {
          while (readUByte(in) != 0)
            ;
        }
        // Skip optional file comment
        if ((flg & FCOMMENT) == FCOMMENT) {
          while (readUByte(in) != 0)
            ;
        }
        // Check optional header CRC
        if ((flg & FHCRC) == FHCRC) {
          int v = (int) _crc.getValue() & 0xffff;
          if (readUShort(in) != v) {
            throw new IOException("Corrupt GZIP header");
          }
        }
        return true;
      } catch (EOFException e) {
      }
    }
    return false;
  }

  /*
   * Reads GZIP member trailer.
   */
  private void readTrailer() throws IOException {

    PushbackInputStream in = (PushbackInputStream) this.in;

    int n = inf.getRemaining();

    if (n > 0) {
      in.unread(buf, len - n, n);
    }
    // Uses left-to-right evaluation order
    if ((readUInt(in) != _crc.getValue()) ||
    // rfc1952; ISIZE is the input size modulo 2^32
        (readUInt(in) != (inf.getBytesWritten() & 0xffffffffL)))
      throw new IOException("Corrupt GZIP trailer");
  }

  /*
   * Reads unsigned integer in Intel byte order.
   */
  private static long readUInt(InputStream in) throws IOException {
    long s = readUShort(in);
    return ((long) readUShort(in) << 16) | s;
  }

  /*
   * Reads unsigned short in Intel byte order.
   */
  private static int readUShort(InputStream in) throws IOException {
    int b = readUByte(in);
    return ((int) readUByte(in) << 8) | b;
  }

  /*
   * Reads unsigned byte.
   */
  private static int readUByte(InputStream in) throws IOException {
    int b = in.read();
    if (b == -1) {
      throw new EOFException();
    }
    if (b < -1 || b > 255) {
      // Report on this.in, not argument in; see read{Header, Trailer}.
      throw new IOException("read() returned value out of range -1..255: " + b);
    }
    return b;
  }

  private byte[] tmpbuf = new byte[128];

  /*
   * Skips bytes of input data blocking until all bytes are skipped. Does not
   * assume that the input stream is capable of seeking.
   */
  private void skipBytes(InputStream in, int n) throws IOException {
    while (n > 0) {
      int len = in.read(tmpbuf, 0, n < tmpbuf.length ? n : tmpbuf.length);
      if (len == -1) {
        throw new EOFException();
      }
      n -= len;
    }
  }

  /**
   * calculated raw arc file stream pos (taking into account any buffered data
   * contained within PushBackInputStream
   * 
   * @return current stream position in bytes
   * @throws IOException
   *           if error occurs
   */
  private final int getARCFileStreamPos() throws IOException {
    PushbackInputStream in = (PushbackInputStream) this.in;
    return _streamPos - in.available();
  }

  /**
   * 
   * ArcFileBuilder helper class - used to construct ArcFileItem objects from an
   * ARC File Entry in a stateful manner
   * 
   */
  public static class ArcFileBuilder {

    // various states of processing an ARC FILE
    private enum State {
      LookingForMetadata, LookingForHeaderTerminator, ReadingContent, Finished
    }

    // ARC FILE HEADER TIMESTAMP FORMAT
    // Note: Not Thread-Safe, so every instance of builder needs its own copy
    // ...
    SimpleDateFormat       TIMESTAMP14   = new SimpleDateFormat(
                                             "yyyyMMddHHmmss");
    // ArcFileItem this builder returns
    ArcFileItem            _item         = null;
    // the content buffer associated this item ...
    Buffer                 _buffer       = new Buffer();
    // Builder State
    State                  _state        = State.LookingForMetadata;
    // Queued Input State
    LinkedList<ByteBuffer> _buffers      = new LinkedList<ByteBuffer>();
    // Active Input Buffer
    ByteBuffer             _activeBuffer = null;
    // last matched pattern char
    byte                   lastMatchChar = 0;
    // match count
    int                    matchCount    = 0;

    // End Of Stream Indicator
    boolean                eos           = false;
    // Charsets used during decoding process
    static Charset         UTF8_Charset  = Charset.forName("UTF8");
    static Charset         ASCII_Charset = Charset.forName("ASCII");

    /**
     * Constructor
     * 
     * @param itemToConstruct
     *          - the ArcFileItem to build
     */
    public ArcFileBuilder(ArcFileItem itemToConstruct) {
      _item = itemToConstruct;
    }

    private final boolean checkForCRLFTerminator(byte matchingChar) {
      if (matchingChar == '\n') {
        switch (matchCount) {
          case 1:
            ++matchCount;
            break;
          case 3:
            matchCount = 0;
            return true;
          default:
            matchCount = 0;
        }
      } else if (matchingChar == '\r') {
        switch (matchCount) {
          case 2:
            ++matchCount;
            break;
          default:
            matchCount = 1;
            break;
        }
      } else {
        matchCount = 0;
      }
      return false;
    }

    /** check for terminator pattern **/
    private final boolean checkForTerminator(byte matchingChar) {

      boolean terminatorFound = false;

      switch (_state) {
        // metadata line is terminated by a single line-feed
        case LookingForMetadata: {
          if (matchingChar == '\n') {
            terminatorFound = true;
          }
        }
          break;

        // http headers are terminated by the standard crlf-crlf pattern
        case LookingForHeaderTerminator: {
          terminatorFound = checkForCRLFTerminator(matchingChar);
        }
          break;
      }

      if (terminatorFound) {
        // reset state ...
        matchCount = 0;
        // if active buffer contains no more characters...
        if (_activeBuffer.remaining() == 0) {
          // add entire active buffer to input state
          _activeBuffer.rewind();
          _buffers.addLast(_activeBuffer);
          _activeBuffer = null;
        } else {
          // otherwise, slice buffer at current position, and
          // add one buffer to input state, and make the other current
          ByteBuffer oldBuffer = _activeBuffer;
          _activeBuffer = _activeBuffer.slice();
          oldBuffer.limit(oldBuffer.position());
          oldBuffer.rewind();
          _buffers.addLast(oldBuffer);
        }
      }
      return terminatorFound;
    }

    /**
     * newInputStream
     * 
     * @param buf
     *          - ByteBuffer to wrap as an InputStream
     * @return InputStream - wrapped InputStream object
     */
    private static InputStream newInputStream(final ByteBuffer buf) {
      return new InputStream() {
        public synchronized int read() throws IOException {
          if (!buf.hasRemaining()) {
            return -1;
          }
          return buf.get();
        }

        public synchronized int read(byte[] bytes, int off, int len)
            throws IOException {
          // Read only what's left
          len = Math.min(len, buf.remaining());
          buf.get(bytes, off, len);
          return len;
        }
      };
    }

    /** construct a reader given a list of ByteBuffers **/
    private static InputStreamReader readerFromScanBufferList(
        LinkedList<ByteBuffer> buffers, Charset charset) throws IOException {
      Vector<InputStream> inputStreams = new Vector<InputStream>();

      for (ByteBuffer buffer : buffers) {
        inputStreams.add(newInputStream(buffer));
      }
      buffers.clear();

      SequenceInputStream seqInputStream = new SequenceInputStream(inputStreams
          .elements());
      ;

      return new InputStreamReader(seqInputStream, charset);
    }

    /** construct a single line from the current input state **/
    private final String readLine(Charset charset) throws IOException {

      BufferedReader reader = new BufferedReader(readerFromScanBufferList(
          _buffers, charset));

      return reader.readLine();
    }

    /** process the metadata line of an ARC File Entry **/
    private final void processMetadataLine(String metadata) throws IOException {

      StringTokenizer tokenizer = new StringTokenizer(metadata, " ");
      int tokenCount = 0;
      while (tokenizer.hasMoreElements() && tokenCount <= 5) {
        switch (++tokenCount) {

          // URI
          case 1: {
            _item.setUri(tokenizer.nextToken());
          }
            break;

          // Host IP Address
          case 2: {
            _item.setHostIP(tokenizer.nextToken());
          }
            break;

          // Timestamp
          case 3: {
            String timeStamp = tokenizer.nextToken();
            try {
              _item.setTimestamp(TIMESTAMP14.parse(timeStamp).getTime());
            } catch (ParseException e) {
              LOG.error("Invalid Timestamp Encountered in Item Metdata. URL:"
                  + _item.getUri() + " Timestamp:" + timeStamp + " Metadata:"
                  + metadata);
              _item.setTimestamp(0);
            }
          }
            break;

          // MimeType
          case 4: {
            _item.setMimeType(tokenizer.nextToken());
          }
            break;

          // and Record Length
          case 5: {
            _item.setRecordLength(Integer.parseInt(tokenizer.nextToken()));
          }
            break;
        }
      }
    }

    /** extract http headers from the current input state **/
    private final void processHeaders() throws IOException {

      BufferedReader reader = new BufferedReader(readerFromScanBufferList(
          _buffers, ArcFileBuilder.UTF8_Charset));

      String line = null;

      _item.setFieldDirty(ArcFileItem.Field_HEADERITEMS);

      while ((line = reader.readLine()) != null) {
        if (line.length() != 0) {
          int colonPos = line.indexOf(':');

          ArcFileHeaderItem item = new ArcFileHeaderItem();

          if (colonPos != -1 && colonPos != line.length() - 1) {

            item.setItemKey(line.substring(0, colonPos));
            item.setItemValue(line.substring(colonPos + 1));

            // if this is our special truncation flag ...
            if (item.getItemKey().equals(
                Constants.ARCFileHeader_ContentTruncated)) {
              String parts[] = item.getItemValue().split(",");
              for (String part : parts) {
                if (part.equals(ArcFileItem.Flags
                    .toString(ArcFileItem.Flags.TruncatedInInflate))) {
                  _item.setFlags(_item.getFlags()
                      | ArcFileItem.Flags.TruncatedInDownload);
                } else if (part.equals(ArcFileItem.Flags
                    .toString(ArcFileItem.Flags.TruncatedInInflate))) {
                  _item.setFlags(_item.getFlags()
                      | ArcFileItem.Flags.TruncatedInInflate);
                }
              }
            }
          } else {
            item.setItemValue(line);
          }
          _item.getHeaderItems().add(item);
        }
      }
    }

    /** transition from the current input state to the next input state **/
    private final void transitionState() throws IOException {

      switch (_state) {

        case LookingForMetadata: {
          // decode the string as a utf-8 string
          processMetadataLine(readLine(ASCII_Charset));
          // and advance to next state ...
          _state = ArcFileBuilder.State.LookingForHeaderTerminator;
        }
          break;
        case LookingForHeaderTerminator: {
          // found header terminator
          processHeaders();
          // and advance to next state ...
          _state = ArcFileBuilder.State.ReadingContent;
          // and set up arc file item for read ...
          _buffer.setCapacity(_blockSize);
        }
          break;
      }
    }

    /**
     * inform builder that input for the current item has been exhauseted
     * 
     * @return ArcFileItem - the fully constructed ArcFileItem object if
     *         construction was successfull
     * @throws IOException
     *           - if building fails
     */
    public final void finish() throws IOException {
      if (_state == State.ReadingContent) {
        _state = State.Finished;
        // generate warning in case of zero content edge case ...
        if (_buffer.getCount() == 0) {
          LOG
              .error("ArcFileBuilder Encountered Item with Zero Length Content. URI:"
                  + _item.getUri());
        } else {
          _item.setContent(_buffer);
          _buffer = new Buffer();
        }
        _item = null;
      } else {
        throw new IOException(
            "ArcBuilder finish calledin Invalid State. State:" + _state
                + " ArcFile:" + _item.getArcFileName() + " Position:"
                + _item.getArcFilePos() + " Item URI:" + _item.getUri());
      }
    }

    /**
     * Input Data into the builder
     * 
     * @param buffer
     *          - a piece of uncompressed content
     * @throws IOException
     *           - throws exception if building fails
     */
    public final void inputData(ByteBuffer buffer) throws IOException {

      // set the buffer as the active buffer ...
      _activeBuffer = buffer;

      // scan looking for terminator
      while (_activeBuffer != null && _activeBuffer.remaining() != 0) {

        // if not reading content then
        if (_state != ArcFileBuilder.State.ReadingContent) {

          // read a byte at a time ...
          byte b = _activeBuffer.get();

          // and if the byte is a delimiter ...
          if (b == '\r' || b == '\n') {

            // and check for pattern match (terminator match)
            if (checkForTerminator(b)) {
              transitionState();
            }
          }
          // otherwise reset pattern buffer
          else {
            matchCount = 0;
          }
        } else {
          // calculate available storage in buffer ...
          int available = _buffer.getCapacity() - _buffer.getCount();
          // if we need more room ...
          if (available < _activeBuffer.remaining()) {
            // figure out how much to grow buffer by ...
            int growByAmount = Math.max(_activeBuffer.remaining() - available,
                _blockSize * 2);
            // and grow the buffer ...
            _buffer.setCapacity(_buffer.getCapacity() + growByAmount);
          }
          // copy the buffer data in one go ...
          _buffer.append(_activeBuffer.array(), _activeBuffer.position()
              + _activeBuffer.arrayOffset(), _activeBuffer.remaining());
          _activeBuffer = null;
        }
      }
      // now if we reached the end of the buffer while scanning for a token ...
      if (_activeBuffer != null) {
        // add entire buffer to buffer list ...
        _activeBuffer.rewind();
        _buffers.add(_activeBuffer);
        _activeBuffer = null;
      }
    }
  }

  // ////////////////////////////////////////////////////////////////////////////////
  // test routines
  // ////////////////////////////////////////////////////////////////////////////////

  public void checkCRLFStateMachine() throws Exception {

    ArcFileItem item = new ArcFileItem();
    ArcFileBuilder builder = new ArcFileBuilder(item);

    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\n'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertTrue(builder.checkForCRLFTerminator((byte) '\n'));

    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\n'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\n'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertTrue(builder.checkForCRLFTerminator((byte) '\n'));

    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\n'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\n'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertTrue(builder.checkForCRLFTerminator((byte) '\n'));

    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\n'));
    Assert.assertFalse(builder.checkForCRLFTerminator((byte) '\r'));
    Assert.assertTrue(builder.checkForCRLFTerminator((byte) '\n'));

  }

  @Test
  public void testReader(File file) throws Exception {

    checkCRLFStateMachine();

    setIOTimeoutValue(30000);

    resetState();

    Thread thread = new Thread(new Runnable() {

      public void run() {
        try {

          while (hasMoreItems()) {
            ArcFileItem item = new ArcFileItem();

            getNextItem(item);

            LOG.info("GOT Item URL:" + item.getUri() + " StreamPos:"
                + item.getArcFilePos() + " Content Length:"
                + item.getContent().getCount());
            for (ArcFileHeaderItem headerItem : item.getHeaderItems()) {
              if (headerItem.isFieldDirty(ArcFileHeaderItem.Field_ITEMKEY)) {
                // LOG.info("Header Item:" + headerItem.getItemKey() + " :" +
                // headerItem.getItemValue());
              } else {
                // LOG.info("Header Item:" + headerItem.getItemValue());
              }
            }
            // LOG.info("Content Length:" + item.getContent().getCount());
            // LOG.info("Content:");
            /*
             * ByteArrayInputStream inputStream = new
             * ByteArrayInputStream(item.getContent
             * ().getReadOnlyBytes(),0,item.getContent().getCount());
             * BufferedReader reader = new BufferedReader(new
             * InputStreamReader(inputStream,Charset.forName("ASCII"))); String
             * line = null; while ((line = reader.readLine()) != null) {
             * LOG.info(line); }
             */
          }
          LOG.info("NO MORE ITEMS... BYE");
        } catch (IOException e) {
          LOG.error(StringUtils.stringifyException(e));
        }
      }

    });

    // run the thread ...
    thread.start();

    ReadableByteChannel channel = Channels
        .newChannel(new FileInputStream(file));

    try {

      int totalBytesRead = 0;
      for (;;) {

        ByteBuffer buffer = ByteBuffer.allocate(32768);

        int bytesRead = channel.read(buffer);
        // LOG.info("Read "+bytesRead + " From File");

        if (bytesRead == -1) {
          finished();
          break;
        } else {
          buffer.flip();
          totalBytesRead += buffer.remaining();
          available(buffer);
        }
      }
    } finally {
      channel.close();
    }

    // now wait for thread to die ...
    LOG.info("Done Reading File.... Waiting for ArcFileThread to DIE");
    thread.join();
    LOG.info("Done Reading File.... ArcFileThread to DIED");
  }

  public static void main(String[] args) {
    File file = new File(args[0]);

    ArcFileReader reader = new ArcFileReader();
    try {
      reader.testReader(file);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
