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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.commoncrawl.crawl.common.shared.Constants;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.ByteArrayUtils;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.GZIPUtils;
import org.commoncrawl.util.shared.GZIPUtils.UnzipResult;
import org.commoncrawl.util.shared.IPAddressUtils;
import org.commoncrawl.util.shared.Tuples.Pair;
import org.junit.Test;

import com.google.common.collect.Lists;

/** 
 * ARCFileReader tests
 * 
 * @author rana
 *
 */
public class ArcFileReaderTests {

  private static final Log              LOG                      = LogFactory
      .getLog(ArcFileReaderTests.class);
  
  static String getMetaLine(String uri,String arcFileName, String contentType,
      String hostIP, long fetchBeginTimeStamp, long recordLength)
      throws IOException {

    if (fetchBeginTimeStamp <= 0) {
      throw new IOException("Bogus fetchBeginTimestamp: "
          + Long.toString(fetchBeginTimeStamp));
    }

    return createMetaline(uri, arcFileName,hostIP, TIMESTAMP14.format(new Date(
        fetchBeginTimeStamp)), contentType, Long.toString(recordLength));
  }
  
  static SimpleDateFormat       TIMESTAMP14              = new SimpleDateFormat(
      "yyyyMMddHHmmss");

  static final char             HEADER_FIELD_SEPARATOR   = ' ';
  static final String           UTF8                     = "UTF-8";
  static final char             LINE_SEPARATOR           = '\n';
  static final byte[]           ARC_GZIP_EXTRA_FIELD     = { 8, 0, 'L',
    'X', 4, 0, 0, 0, 0, 0                                     };
  static final String           DEFAULT_ENCODING         = "ISO-8859-1";
  static final String           ARC_MAGIC_NUMBER         = "filedesc://";

  /**
   * An override so we get access to underlying output stream and offer an end()
   * that does not accompany closing underlying stream.
   * 
   * @author stack
   */
  static class CompressedStream extends GZIPOutputStream {
    public CompressedStream(OutputStream out) throws IOException {
      super(out);
    }

    /**
     * @return Reference to stream being compressed.
     */
    OutputStream getWrappedStream() {
      return this.out;
    }

    /**
     * Release the deflater's native process resources, which otherwise would
     * not occur until either finalization or DeflaterOutputStream.close()
     * (which would also close underlying stream).
     */
    public void end() {
      def.end();
    }
  }
  
  static String createMetaline(String uri,String arcFileName, String hostIP,
      String timeStamp, String mimetype, String recordLength) {
    return uri + HEADER_FIELD_SEPARATOR + hostIP + HEADER_FIELD_SEPARATOR
        + timeStamp + HEADER_FIELD_SEPARATOR + mimetype
        + HEADER_FIELD_SEPARATOR + recordLength + LINE_SEPARATOR;
  }
  
  static byte[] generateARCFileMetaData(String arcFileName,String date) throws IOException {

    String metadataHeaderLinesTwoAndThree = getMetadataHeaderLinesTwoAndThree("1 "
        + "0");
    int recordLength = metadataHeaderLinesTwoAndThree
        .getBytes(DEFAULT_ENCODING).length;
    String metadataHeaderStr = ARC_MAGIC_NUMBER + arcFileName
        + " 0.0.0.0 " + date + " text/plain " + recordLength
        + metadataHeaderLinesTwoAndThree;

    ByteArrayOutputStream metabaos = new ByteArrayOutputStream(recordLength);

    // Write the metadata header.
    metabaos.write(metadataHeaderStr.getBytes(DEFAULT_ENCODING));
    // Write out a LINE_SEPARATORs to end this record.
    metabaos.write(LINE_SEPARATOR);

    // Now get bytes of all just written and compress if flag set.
    byte[] bytes = metabaos.toByteArray();

    // GZIP the header but catch the gzipping into a byte array so we
    // can add the special IA GZIP header to the product. After
    // manipulations, write to the output stream (The JAVA GZIP
    // implementation does not give access to GZIP header. It
    // produces a 'default' header only). We can get away w/ these
    // maniupulations because the GZIP 'default' header doesn't
    // do the 'optional' CRC'ing of the header.

    byte[] gzippedMetaData = gzip(bytes);

    if (gzippedMetaData[3] != 0) {
      throw new IOException("The GZIP FLG header is unexpectedly "
          + " non-zero.  Need to add smarter code that can deal "
          + " when already extant extra GZIP header fields.");
    }

    // Set the GZIP FLG header to '4' which says that the GZIP header
    // has extra fields. Then insert the alex {'L', 'X', '0', '0', '0,
    // '0'} 'extra' field. The IA GZIP header will also set byte
    // 9 (zero-based), the OS byte, to 3 (Unix). We'll do the same.
    gzippedMetaData[3] = 4;
    gzippedMetaData[9] = 3;

    byte[] assemblyBuffer = new byte[gzippedMetaData.length
        + ARC_GZIP_EXTRA_FIELD.length];
    // '10' in the below is a pointer past the following bytes of the
    // GZIP header: ID1 ID2 CM FLG + MTIME(4-bytes) XFL OS. See
    // RFC1952 for explaination of the abbreviations just used.
    System.arraycopy(gzippedMetaData, 0, assemblyBuffer, 0, 10);
    System.arraycopy(ARC_GZIP_EXTRA_FIELD, 0, assemblyBuffer, 10,
        ARC_GZIP_EXTRA_FIELD.length);
    System.arraycopy(gzippedMetaData, 10, assemblyBuffer,
        10 + ARC_GZIP_EXTRA_FIELD.length, gzippedMetaData.length - 10);
    bytes = assemblyBuffer;

    //System.out.println("Header Bytes:" + HexDump.dumpHexString(bytes));
    return bytes;
  }
  
  static String getMetadataHeaderLinesTwoAndThree(String version) {
    StringBuffer buffer = new StringBuffer();
    buffer.append(LINE_SEPARATOR);
    buffer.append(version);
    buffer.append(" CommonCrawl");
    buffer.append(LINE_SEPARATOR);
    buffer.append("URL IP-address Archive-date Content-type Archive-length");
    buffer.append(LINE_SEPARATOR);
    return buffer.toString();
  }
  
  /**
   * Gzip passed bytes. Use only when bytes is small.
   * 
   * @param bytes
   *          What to gzip.
   * @return A gzip member of bytes.
   * @throws IOException
   */
  static byte[] gzip(byte[] bytes) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gzipOS = new GZIPOutputStream(baos);
    gzipOS.write(bytes, 0, bytes.length);
    gzipOS.close();
    return baos.toByteArray();
  }
  
  
  public static void writeFirstRecord(final OutputStream os,final String fileName,long ts) throws IOException {
    os.write(generateARCFileMetaData(fileName,TIMESTAMP14.format(new Date(System.currentTimeMillis()))));
  }

  public static boolean write(OutputStream os,String normalizedURL,String arcFileName, int segmentid, int crawlNumber,byte[] crawlData,int crawlDataOffset,int crawlDataLen,NIOHttpHeaders headers, String contentType,
      String signature, int hostIP,long lastAttemptTime) throws IOException {

    String encodedURI = normalizedURL;

    String hostIPStr = IPAddressUtils.IntegerToIPAddressString(hostIP);
    long fetchBeginTimestamp = lastAttemptTime;
    String encoding = headers.findValue("Content-Encoding");
    String truncationFlags = "";


    {

      if (crawlData != null && encoding != null
          && encoding.equalsIgnoreCase("gzip")) {
        int compressedSize = crawlData.length;
        try {
          UnzipResult result = GZIPUtils.unzipBestEffort(crawlData,2 << 20);

          crawlData = result.data;
          crawlDataOffset = 0;
          crawlDataLen = result.data.length;

          if (result.wasTruncated) {
            if (truncationFlags.length() != 0)
              truncationFlags += ",";
            truncationFlags += ArcFileItem.Flags
                .toString(ArcFileItem.Flags.TruncatedInInflate);
          }
        } catch (Exception e) {
          LOG.error("URL:" + normalizedURL
              + " Rejected - GZIP Decompression Failed");
          crawlData = null;
        }
      }

      // content must not be null
      if (crawlData == null) {
        LOG.error("URL:" + normalizedURL + " Rejected - Content is NULL");
      } else {

        // add in our custom headers ...
        headers.add(Constants.ARCFileHeader_ParseSegmentId,
            ((Integer) segmentid).toString());
        headers.add(Constants.ARCFileHeader_OriginalURL, normalizedURL);

        headers.add(Constants.ARCFileHeader_Signature, signature);
        headers.add(Constants.ARCFileHeader_CrawlNumber, Integer
            .toString(crawlNumber));
        headers.add(Constants.ARCFileHeader_FetchTimeStamp, Long
            .toString(fetchBeginTimestamp));
        // headers.add(Environment.ARCFileHeader_CrawlerId,
        // Integer.toString((int)urlItem.get));

        if (truncationFlags.length() != 0) {
          headers
              .add(Constants.ARCFileHeader_ContentTruncated, truncationFlags);
        }

        String headerString = headers.toString() + "\r\n";

        byte[] headerBytes = headerString.getBytes("UTF-8");

        // content is truncated further upstream, so this redundant check /
        // truncation is problematic
        // int contentLength = Math.min(crawlData.length,CONTENT_SIZE_LIMIT);

        // extract metadata line upfront, since if the url exceeds a certain
        // size limit , we are going to reject the entry...
        byte metaDataLine[];

        try {
          metaDataLine = getMetaLine(encodedURI,arcFileName, contentType, hostIPStr,
              fetchBeginTimestamp, crawlDataLen + headerBytes.length).getBytes(
              UTF8);
        } catch (IOException e) {
          LOG.error("Metadata Line Validation FAILED with Exception:"
              + CCStringUtils.stringifyException(e));
          // bail here ...
          return false;
        }

        // get ready to write out a new gziped entry ...
        OutputStream compressedStream = preWriteRecordTasks(os,headerBytes.length, crawlDataLen, contentType);
        try {
          // read to write an entry ...
          compressedStream.write(metaDataLine);

          // write out the headers ...
          compressedStream.write(headerBytes, 0, headerBytes.length);
          // write out the content
          compressedStream.write(crawlData, 0, crawlDataLen);
          // line separator ...
          compressedStream.write(LINE_SEPARATOR);

        } finally {
          // flush the gzip stream...
          postWriteRecordTasks(compressedStream);
        }
      }
      
      return true;
    }
    
  }
  
  static OutputStream preWriteRecordTasks(OutputStream os,int headerBytesLength,
      int contentBytesLength, String contentType) throws IOException {

    // Wrap stream in GZIP Writer.
    // The below construction immediately writes the GZIP 'default'
    // header out on the underlying stream.
    return new CompressedStream(os);
  }

  static OutputStream postWriteRecordTasks(OutputStream os) throws IOException {
    CompressedStream o = (CompressedStream) os;
    o.finish();
    o.flush();
    o.end();
    return o.getWrappedStream();
  }
  static String randomConstrainedString(final Random random,char validChars[],final int minLength, final int maxLength) {
    final int length = random.nextInt(maxLength - minLength) + minLength;
    final char[] chars = new char[length];
    for (int i = 0, x = chars.length; i < x; )
      chars[i++] = validChars[random.nextInt(validChars.length)];
    return new String(chars);
  }
  
  static String randomString(final Random random,
      final int minLength, final int maxLength) {
      final int length = random.nextInt(maxLength - minLength) + minLength;
      final char[] chars = new char[length];
      for (int i = 0, x = chars.length; i < x; )
          do {
          final int cp = random.nextInt(0x10FFFF + 1);
            if (!Character.isDefined(cp))
                continue;
            final char[] chs = Character.toChars(cp);
            if (chs.length > x - i)
                continue;
            for (final char ch : chs) {
              if (!Character.isWhitespace(ch)) { 
                  chars[i++] = ch;
              }
            }
            break;
          
      } while (true);

    return new String(chars);
  }

  static final String[] testHeaderKeys = { 
    "x-cc-test-header-1",
    "x-cc-test-header-2",
    "x-cc-test-header-3"
  };
  
  static final String validHeaderChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  public static class TestRecord { 
    public String url;
    public byte[] data;
    public List<Pair<String,String>> headers;
    public int streamPos;
    public int rawSize; 
  }
  
  
  public static List<TestRecord> buildTestRecords(int recordCount) {
    Random random = new Random();
    
    List<TestRecord> records = Lists.newArrayList();
    
    char headerChars[] = validHeaderChars.toCharArray();
    
    for (int i=0;i<recordCount;++i) {
      TestRecord record = new TestRecord();
      // intentionally add a space in the url to mimic malformed headers
      record.url = "http://foo/ " + randomString(random, 5, 100);
      record.data = randomString(random, 1000, 3000).getBytes(Charset.forName("UTF-8"));
      record.headers = Lists.newArrayList();
      for (int j=0;j<testHeaderKeys.length;++j) { 
        record.headers.add(new Pair<String, String>(testHeaderKeys[j],randomConstrainedString(random, headerChars, 100, 200)));
      }
      records.add(record);
    }
    return records;
  }
  
  public static final int BASIC_TEST_RECORD_COUNT = 100;
  
  /** 
   * test basic reader functionality by creating a mock ARCFile in memory and then reading it back and validating the contents... 
   */
  @Test
  public void testReader() {
    DataOutputBuffer os = new DataOutputBuffer();
    long timestamp = System.currentTimeMillis();
    try { 
      // write the ARC File into memory 
      writeFirstRecord(os, "test", timestamp);
      List<TestRecord> records = buildTestRecords(BASIC_TEST_RECORD_COUNT);
      long testAttemptTime = System.currentTimeMillis();
      
      for (TestRecord record : records) { 
        NIOHttpHeaders headers = new NIOHttpHeaders();
        for (int i=0;i<record.headers.size();++i) { 
          headers.set(record.headers.get(i).e0,record.headers.get(i).e1);
        }
        
        write(os,record.url,"test",1,1,record.data,0,record.data.length,headers,"text/html",MD5Hash.digest(record.data).toString(),12345,testAttemptTime);
      }
      os.flush();
      os.close();
      
      final AtomicBoolean streamClosed = new AtomicBoolean();
      // setup ArcFileReader to read the file 
      InputStream in = new ByteArrayInputStream(os.getData(),0,os.getLength()) {
        
        public synchronized int read(byte b[], int off, int len) {
          len = 1;
          return super.read(b, off, len);
        }
        
        public void close() throws IOException {
          super.close();
          streamClosed.set(true);
        }
      };
      ARCFileReader reader = new ARCFileReader(in);
      int index = 0;
      Text key = new Text();
      BytesWritable value = new BytesWritable();
      
      // iterate and validate stuff ... 
      while (reader.hasMoreItems()) {
        reader.nextKeyValue(key, value);
        TestRecord testRecord = records.get(index++);
        // get test key bytes as utf-8 bytes ... 
        byte[] testKeyBytes = testRecord.url.getBytes(Charset.forName("UTF-8"));
        // compare against raw key bytes to validate key is the same (Text's utf-8 mapping code replaces invalid characters 
        // with ?, which causes our test case (which does use invalid characters to from the key, to break.
        Assert.assertTrue(compareTo(testKeyBytes,0,testKeyBytes.length,key.getBytes(),0,key.getLength()) == 0);
        // retured bytes represent the header(encoded in utf-8), terminated by a \r\n\r\n. The content follows this terminator
        // we search for this specific byte pattern to locate start of content, then compare it against source ... 
        int indexofHeaderTerminator = ByteArrayUtils.indexOf(value.getBytes(), 0, value.getLength(), "\r\n\r\n".getBytes());
        if (indexofHeaderTerminator == -1) { 
          throw new IOException("No Header Terminator found in Value!");
        }
        indexofHeaderTerminator += 4;
        // read headers ... 
        String headersText = new String(value.getBytes(),0,indexofHeaderTerminator,Charset.forName("UTF-8"));
        NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(headersText);
        for (int i=0;i<testRecord.headers.size();++i) { 
          Pair<String,String> testHeaderRecord = testRecord.headers.get(i);
          Assert.assertNotNull(headers.findValue(testHeaderRecord.e0));
          Assert.assertEquals(testHeaderRecord.e1,headers.findValue(testHeaderRecord.e0));
        }
        
        Assert.assertTrue(compareTo(testRecord.data,0,testRecord.data.length,value.getBytes(),indexofHeaderTerminator,testRecord.data.length) == 0);
      }
      reader.close();
      
      Assert.assertEquals(index,BASIC_TEST_RECORD_COUNT);
      Assert.assertTrue(streamClosed.get());
    }
    catch (IOException e) { 
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
  
  /** 
   * helper offset based byte array comparator 
   * @param buffer1
   * @param offset1
   * @param length1
   * @param buffer2
   * @param offset2
   * @param length2
   * @return
   */
  public static int compareTo(byte[] buffer1, int offset1, int length1,
      byte[] buffer2, int offset2, int length2) {
    // Short circuit equal case
    if (buffer1 == buffer2 &&
        offset1 == offset2 &&
        length1 == length2) {
      return 0;
    }
    // Bring WritableComparator code local
    int end1 = offset1 + length1;
    int end2 = offset2 + length2;
    for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
      int a = (buffer1[i] & 0xff);
      int b = (buffer2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }
  
  
}
