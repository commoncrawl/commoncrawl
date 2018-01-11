package org.commoncrawl.util.shared;

/**
* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 **/

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PushbackInputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.commoncrawl.crawl.common.shared.Constants;
import org.commoncrawl.io.shared.NIOHttpHeaders;

/**
 * Reads an ARC File via an InputStream, and returns the decompressed content as ArcFileItems
 * 
 * @author rana
 * 
 */
public final class ARCFileReader extends InflaterInputStream {

  // ////////////////////////////////////////////////////////////////////////////////
  // data members
  // ////////////////////////////////////////////////////////////////////////////////

  /** logging **/
  private static final Log LOG = LogFactory.getLog(ARCFileReader.class);


  /** 32 bit crc **/
  private CRC32                           _crc                      = new CRC32();
  /** the arc file header , when available **/
  @SuppressWarnings("unused")
  private String                          _arcFileHeader            = null;
  /** block size used for various operations **/
  public static final int                 DEFAULT_BLOCK_SIZE        = 32 * 1024;

  /** allocation block size **/
  private static int                      _blockSize                = DEFAULT_BLOCK_SIZE;
  

  //////////////////////////////////////////////////////////////////////////////////
  // public API
  // ////////////////////////////////////////////////////////////////////////////////
  
  
  /** 
   * constructor is now private. use the factory method above to construct a reader 
   * @param source
   * @throws IOException
   */
  public ARCFileReader(final InputStream source)throws IOException {
    super(new CustomPushbackInputStream(new CountingInputStream(source),
        _blockSize), new Inflater(true), _blockSize);
    readARCHeader();
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
    try { 
      readHeader();
      return true;
    }
    catch (EOFException e) { 
      return false;
    }
  }

  /**
   * blocking call to retrieve next ArcFileItem contained within an ARC File
   * 
   * @return Fully constructed ArcFileItem
   * @throws IOException
   */
  public void nextKeyValue(Text key,BytesWritable value) throws IOException {

    // read content
    _crc.reset();
    // and reset inflater
    resetInflater();

    // save the arc file stream positon up front
    long streamPos = getPosition();
    
    ArcFileBuilder builder = new ArcFileBuilder(streamPos,key,value);

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

        return;
      }
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
  private void readHeader() throws IOException {

    CheckedInputStream in = new CheckedInputStream(this.in, _crc);

    _crc.reset();

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
    
    long crcValue = readUInt(in);
    long rawByteLen = readUInt(in);
    
    // Uses left-to-right evaluation order
    if ((crcValue != _crc.getValue()) ||
    // rfc1952; ISIZE is the input size modulo 2^32
        (rawByteLen != (inf.getBytesWritten() & 0xffffffffL)))
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
  public final long getPosition() throws IOException {
    CustomPushbackInputStream in = (CustomPushbackInputStream) this.in;
    int rawStreamPos = (int) ((CountingInputStream)in.getSource()).getPosition();
    int bufferedAmt  = in.getAvailableInBuffer();
    int bytesRead = rawStreamPos - bufferedAmt;
    return  bytesRead + 1;
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
    // 
    Text                  _keyOut;
    BytesWritable         _valueOut;
    
    NIOHttpHeaders        _headers = new NIOHttpHeaders();
    // record length 
    int                   _recordLen;
    // arc file payload length  
    int                   _payloadLength;
    // generated header length ... 
    int                   _headerLength;
    // accumulated data length
    int                   _dataLength;
    // Builder State
    State                  _state        = State.LookingForMetadata;
    // Queued Input State
    LinkedList<ByteBuffer> _buffers      = new LinkedList<ByteBuffer>();
    // stream pos 
    long                   _streamPos;
          
    // match count
    enum CRLFMatchState { 
      CRLFMatchState_NoMatch,
      CRLFMatchState_FirstCR,
      CRLFMatchState_FirstLF,
      CRLFMatchState_SecondCR,
    }
    
    CRLFMatchState _crlfMatchState = CRLFMatchState.CRLFMatchState_NoMatch;
      
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
    public ArcFileBuilder(long streamPos,Text key,BytesWritable value) {
      _streamPos = streamPos;
      _keyOut = key;
      _valueOut = value;
    }

    final boolean isHTTPHeaderTermintor(byte c) {
      
      if (c != '\r' && c != '\n') { 
        _crlfMatchState = CRLFMatchState.CRLFMatchState_NoMatch;
      }
      else { 
        switch (_crlfMatchState) { 
        
          case CRLFMatchState_NoMatch: 
          case CRLFMatchState_FirstCR: {
          
            if (c == '\r') 
              _crlfMatchState = CRLFMatchState.CRLFMatchState_FirstCR;
            else if (c == '\n')
              _crlfMatchState = CRLFMatchState.CRLFMatchState_FirstLF;
          }
          break;
          
          case CRLFMatchState_FirstLF: { 
            if (c == '\r') 
              _crlfMatchState = CRLFMatchState.CRLFMatchState_SecondCR;
            else if (c == '\n') 
              return true;
          }
          break;
          
          case CRLFMatchState_SecondCR: { 
            if (c == '\r')  
              _crlfMatchState = CRLFMatchState.CRLFMatchState_FirstCR;
            else if (c == '\n') 
              return true;
          }
          break;
        }
      }
      return false;
    }

    private final ByteBuffer appendAndSliceBuffer(ByteBuffer buffer)throws IOException { 
      // if active buffer contains no more characters...
      if (buffer.remaining() == 0) {
        // add entire active buffer to input state
        buffer.rewind();
        _buffers.addLast(buffer);
        return null;
      } else {
        // otherwise, slice buffer at current position, and
        // add one buffer to input state, and make the other current
        ByteBuffer oldBuffer = buffer;
        buffer = buffer.slice();
        oldBuffer.limit(oldBuffer.position());
        oldBuffer.rewind();
        _buffers.addLast(oldBuffer);
        return buffer;
      }
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
          terminatorFound = isHTTPHeaderTermintor(matchingChar);
        }
        break;
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

    //static final Pattern metadataLinePattern = Pattern.compile("^([^ ]+)[ ]([0-9\\.]+)\\s([0-9]+)\\s([^\\s]+)\\s([0-9]+)$");
    static final Pattern metadataLinePattern = Pattern.compile("^(.*)\\s([0-9\\.]+)\\s([0-9]+)\\s([^\\s]+)\\s([0-9]+)$");
    
    private static String[] parseMetadataLine(String metadataLine) { 
      Matcher m = metadataLinePattern.matcher(metadataLine);
      if (m.matches() && m.groupCount() == 5) { 
        return new String[] { 
            m.group(1),
            m.group(2),
            m.group(3),
            m.group(4),
            m.group(5)
        };
      }
      return null;
    }
    
    private static String[] parseMalformedMetadataLine(String malformedMetadataLine) { 
      String parts[] = new String[5];
      int lastMatchPos=malformedMetadataLine.length();
      int matchCount = 0;
      for (int i=0;i<5;++i) { 
        int nextMatchPos = malformedMetadataLine.lastIndexOf(' ',lastMatchPos -1);
        if (nextMatchPos != -1 || matchCount == 4) {
          matchCount++;
          if (matchCount == 5) { 
            parts[5-matchCount] = malformedMetadataLine.substring(0,lastMatchPos);
          }
          else { 
            parts[5-matchCount] = malformedMetadataLine.substring(nextMatchPos+1,lastMatchPos);
          }
          lastMatchPos = nextMatchPos;
        }
        else { 
          break;
        }
      }
      if (matchCount==5) { 
        return parts;
      }
      return null;
    }
    
    /** process the metadata line of an ARC File Entry **/
    private final void processMetadataLine(String metadata) throws IOException {

      String parts[] = parseMetadataLine(metadata);
      if (parts == null)
        parts = parseMalformedMetadataLine(metadata);
      
      if (parts != null) {
        _keyOut.set(parts[0]);
        _headers.add(Constants.ARCFileHeader_HostIP,parts[1]);
        _headers.add(Constants.ARCFileHeader_ARC_Timestamp,parts[2]);
        _headers.add(Constants.ARCFileHeader_ARC_MimeType,parts[3]);
        _recordLen = Integer.parseInt(parts[4]);
      }
      else { 
        throw new IOException("Invalid ARC Metadata Line:" + metadata);
      }
    }

    /** extract http headers from the current input state **/
    private final void processHeaders() throws IOException {
      
      // ok cache raw header size ... 
      int rawHeaderSize=0;
      for (ByteBuffer buffer : _buffers) { 
        rawHeaderSize += buffer.remaining();
      }
      
      byte[] headerBytes = new byte[rawHeaderSize];
      int offset=0;
      for (ByteBuffer buffer : _buffers) { 
        int len = buffer.remaining();
        buffer.get(headerBytes, offset, len);
        offset+= len;
      }
      
      BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(headerBytes),ArcFileBuilder.UTF8_Charset));
      
      //BufferedReader reader = new BufferedReader(readerFromScanBufferList(_buffers, ArcFileBuilder.UTF8_Charset));
      

      String line = null;

      while ((line = reader.readLine()) != null) {
        if (line.length() != 0) {
          int colonPos = line.indexOf(':');
          if (colonPos != -1 && colonPos != line.length() - 1) {
            _headers.add(line.substring(0, colonPos), line.substring(colonPos + 1));
          } else {
            _headers.add("", line);
          }
        }
      }
      
      // ok now calculate payload size based on header size 
      _payloadLength = Math.max(0, _recordLen - rawHeaderSize);
      if (_payloadLength == 0) { 
        LOG.warn("Invalid Payload Size Detected Key:"+ _keyOut.toString() +" RecordLen:" + _recordLen + " HeaderLen:" + rawHeaderSize + " Headers:\n" + _headers.toString());
        LOG.warn("Header Dump:"+ HexDump.dumpHexString(headerBytes));
      }
      // set into headers ... 
      _headers.set(Constants.ARCFileHeader_ARC_PayloadLen, Integer.toString(_payloadLength));
      // ok write headers to stream... 
      DataOutputBuffer headerBuffer = new DataOutputBuffer();
      OutputStreamWriter headerStream = new OutputStreamWriter(headerBuffer, Charset.forName("UTF-8"));
      headerStream.write(_headers.toString());
      headerStream.write("\r\n");
      headerStream.flush();
      
      // ok cache generated header length ... 
      _headerLength = headerBuffer.getLength();
      
      // ok pre-allocate buffer to accomodate both generated header and payload 
      _valueOut.setSize(_headerLength + _payloadLength);
      // write out header bytes .. 
      System.arraycopy(headerBuffer.getData(),0,_valueOut.getBytes(),0,_headerLength);
    }

    /** transition from the current input state to the next input state **/
    private final void transitionState() throws IOException {

      switch (_state) {

        case LookingForMetadata: {
          // decode the string as a utf-8 string
          processMetadataLine(readLine(Charset.forName("UTF-8")));
          // and advance to next state ...
          _state = ArcFileBuilder.State.LookingForHeaderTerminator;
        }
        break;
        
        case LookingForHeaderTerminator: {
          // found header terminator
          processHeaders();
          // and advance to next state ...
          _state = ArcFileBuilder.State.ReadingContent;
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
        if (_dataLength == 0) {
          LOG.error("ArcFileBuilder Encountered Item with Zero Length Content. URI:"+ _keyOut);
        } 
      } else {
        throw new IOException(
            "ArcBuilder finish calledin Invalid State. State:" + _state
                + " ArcFile:" + _keyOut + " Position:" + _streamPos
                + " Item URI:" + _keyOut);
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

      // if still scanning for headers then ... 
      while(buffer != null && _state != ArcFileBuilder.State.ReadingContent) {
        boolean terminated = false;
        while (buffer.remaining() != 0) { 
          // read a byte at a time ...
          final byte b = buffer.get();

          // and if the byte is a delimiter ...
          if (b == '\r' || b == '\n' || _crlfMatchState != CRLFMatchState.CRLFMatchState_NoMatch) {
            // and check for pattern match (terminator match)
            terminated  = checkForTerminator(b);
            if (terminated){
              // append buffer data up to terminal token position 
              buffer = appendAndSliceBuffer(buffer);
              // transition state ... 
              transitionState();
              break;
            }
          }
        }
        if (!terminated) { 
          if (buffer != null && buffer.remaining() == 0) { 
            buffer = appendAndSliceBuffer(buffer);
          }
        }
      }
      
      if (_state == ArcFileBuilder.State.ReadingContent) { 
        if (buffer != null && buffer.remaining() !=0) { 
            // amount to copy 
            int copyAmount = Math.min(_payloadLength - _dataLength,buffer.remaining());
            if (copyAmount <= 0) {
              /*
              if (buffer.remaining() != 0) { 
                byte debug[] = new byte[buffer.remaining()];
                buffer.get(debug);
                LOG.warn("Trailing Data when Content Already Read for Key:" +_keyOut.toString() +" Size:" + buffer.remaining() + " PayloadLen:" + _payloadLength + " Content:" + HexDump.dumpHexString(debug));
                
              }
              */
            }
            else { 
              // copy the buffer data in one go ...
              buffer.get(_valueOut.getBytes(), _headerLength + _dataLength, copyAmount);
              // increment data length ... 
              _dataLength += copyAmount;
            }
        }
      }
    }
  }

  /** 
   * 
   *  
   * @author rana
   *
   */
  static class CustomPushbackInputStream extends PushbackInputStream {

    public CustomPushbackInputStream(InputStream in, int size) {
      super(in, size);
    }
    
    public InputStream getSource() { 
      return in;
    }
    
    /** 
     * get the exact number of bytes available in the stream's buffer 
     * @return 
     */
    public int getAvailableInBuffer() { 
      return buf.length - pos;
    }
    
    public void debug() { 
      System.out.println(HexDump.dumpHexString(buf, pos, Math.min(buf.length-pos, 100)));
    }
    
  }
  
  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("conf").hasArg().withDescription("Config File Name").create("conf"));
    
    options.addOption(
        OptionBuilder.withArgName("file").hasArg().withDescription("ARC File Path").isRequired().create("file"));
    
    options.addOption(
        OptionBuilder.withArgName("awsAccessKey").hasArg().withDescription("AWS Access Key").create("awsAccessKey"));

    options.addOption(
        OptionBuilder.withArgName("awsSecret").hasArg().withDescription("AWS Secret").create("awsSecret"));

    
  }
  
  static void printUsage() { 
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "ARCFileReaer", options );
  }
  
  public static void main(String[] args)throws IOException, URISyntaxException, InterruptedException {
    
    Configuration conf = new Configuration();

    String path = null;
    
    CommandLineParser parser = new GnuParser();
    
    try { 
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      // get ARCFile Path
      path = cmdLine.getOptionValue("file");
      
      // get optional config 
      if (cmdLine.hasOption("conf")) { 
        conf.addResource(new Path(cmdLine.getOptionValue("conf")));
      }
      if (cmdLine.hasOption("awsAccessKey")) { 
        conf.set("fs.s3n.awsAccessKeyId", cmdLine.getOptionValue("awsAccessKey"));
      }
      if (cmdLine.hasOption("awsSecret")) { 
        conf.set("fs.s3n.awsSecretAccessKey", cmdLine.getOptionValue("awsSecret"));
      }
    }
    catch (ParseException e) { 
      System.out.println(e.toString());
      printUsage();
      System.exit(1);
    }


    final URI uri =  new URI(path);
    FileSystem fs = FileSystem.get(uri,conf);
            
//    byte data[] = new byte[4096*10];
//    int readAmt = 0;
//    while ((readAmt = stream.get().read(data)) != -1) { 
//      System.out.println(HexDump.dumpHexString(data, 0, readAmt));
//    }
//    stream.get().close();
//    System.exit(1);
    
    ARCFileReader reader = null;
    
    try { 
      System.out.println("Initializing Reader for Path:" + uri );
      reader = new ARCFileReader(fs.open(new Path(path)));
      
      Text key = new Text();
      BytesWritable value = new BytesWritable();
      
      while (reader.hasMoreItems()) {
        reader.nextKeyValue(key, value);
        int indexOfTrailingCRLF = ByteArrayUtils.indexOf(value.getBytes(), 0, value.getLength(), "\r\n\r\n".getBytes());
        int headerLen = indexOfTrailingCRLF + 4;
        int contentLen = value.getLength() - headerLen;
        
        String outputStr = "Key:" + key.toString() + " HeaderLen:" + headerLen + " ContentLen:" + contentLen;
        System.out.println(outputStr);
        
        //String contentStr = new String(value.getBytes(),headerLen,contentLen,Charset.forName("ASCII"));
        //System.out.println(contentStr.substring(contentStr.length() - 20));
      }
      System.out.println("Exiting Loop");
    }
    catch (Exception e) {
      System.out.println(CCStringUtils.stringifyException(e));
      LOG.error(CCStringUtils.stringifyException(e));
      //throw new IOException(e);
    }
    finally {
      if (reader != null) { 
        System.out.println("***Closing Reader");
        reader.close();
      }
    }
  }

}
