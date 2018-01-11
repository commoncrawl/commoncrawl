package org.commoncrawl.hadoop.io.mapreduce;

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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.commoncrawl.util.shared.ArcFileReaderTests.TestRecord;
import org.commoncrawl.util.shared.ArcFileReaderTests;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.util.shared.ByteArrayUtils;
import org.junit.Test;

/** 
 * ARCFileRecordReader tests 
 * 
 * @author rana
 *
 */
public class ArcFileRecordReaderTests {
  
  @Test
  public void TestARCFileRecordReader() throws IOException, InterruptedException { 
    
    Configuration conf = new Configuration();
    FileSystem fs = LocalFileSystem.get(conf);
    Path path = new Path("/tmp/" + File.createTempFile("ARCRecordReader", "test"));
    List<TestRecord> records = ArcFileReaderTests.buildTestRecords(ArcFileReaderTests.BASIC_TEST_RECORD_COUNT);
    
    FSDataOutputStream os = fs.create(path);
    try { 
      // write the ARC File into memory 
      ArcFileReaderTests.writeFirstRecord(os, "test", System.currentTimeMillis());
      
      long testAttemptTime = System.currentTimeMillis();
      
      for (TestRecord record : records) { 
        ArcFileReaderTests.write(os,record.url,"test",1,1,record.data,0,record.data.length,new NIOHttpHeaders(),"text/html",MD5Hash.digest(record.data).toString(),12345,testAttemptTime);
      }
      os.flush();
    }
    finally { 
      os.close();
    }
    
    FileSplit split = new FileSplit(path, 0, fs.getFileStatus(path).getLen(), new String[0]);
    ARCFileRecordReader reader = new ARCFileRecordReader();
    reader.initialize(split, new TaskAttemptContext(conf, new TaskAttemptID()));
    
    int index = 0;
    
    // iterate and validate stuff ... 
    while (reader.nextKeyValue()) {
      Text key = reader.getCurrentKey();
      BytesWritable value = reader.getCurrentValue();
      
      TestRecord testRecord = records.get(index++);
      // get test key bytes as utf-8 bytes ... 
      byte[] testKeyBytes = testRecord.url.getBytes(Charset.forName("UTF-8"));
      // compare against raw key bytes to validate key is the same (Text's utf-8 mapping code replaces invalid characters 
      // with ?, which causes our test case (which does use invalid characters to from the key, to break.
      Assert.assertTrue(ArcFileReaderTests.compareTo(testKeyBytes,0,testKeyBytes.length,key.getBytes(),0,key.getLength()) == 0);
      // retured bytes represent the header(encoded in utf-8), terminated by a \r\n\r\n. The content follows this terminator
      // we search for this specific byte pattern to locate start of content, then compare it against source ... 
      int indexofHeaderTerminator = ByteArrayUtils.indexOf(value.getBytes(), 0, value.getLength(), "\r\n\r\n".getBytes());
      indexofHeaderTerminator += 4;
      Assert.assertTrue(ArcFileReaderTests.compareTo(testRecord.data,0,testRecord.data.length,value.getBytes(),indexofHeaderTerminator,testRecord.data.length) == 0);
    }
    reader.close();
    
    Assert.assertEquals(index,ArcFileReaderTests.BASIC_TEST_RECORD_COUNT);
    
    fs.delete(path, false);
  }
}
