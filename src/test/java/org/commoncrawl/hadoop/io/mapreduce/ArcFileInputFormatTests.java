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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.ArcFileReaderTests.TestRecord;
import org.commoncrawl.util.shared.ArcFileItemUtils;
import org.commoncrawl.util.shared.ArcFileReaderTests;
import org.commoncrawl.crawl.common.shared.Constants;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.util.shared.ByteArrayUtils;
import org.commoncrawl.util.shared.Tuples.Pair;
import org.junit.Test;

import com.google.common.collect.Lists;

/** 
 * ARCFileInputFormat tests
 * 
 * @author rana
 *
 */
public class ArcFileInputFormatTests {

  static Pair<Path,List<TestRecord>> buildTestARCFile(Path directoryPath,FileSystem fs, int fileId)throws IOException { 
    List<TestRecord> recordSet = ArcFileReaderTests.buildTestRecords(ArcFileReaderTests.BASIC_TEST_RECORD_COUNT);
    Path filePath = new Path(directoryPath,Integer.toString(fileId)+".arc.gz");
    FSDataOutputStream os = fs.create(filePath);
    try { 
      // write the ARC File into memory 
      ArcFileReaderTests.writeFirstRecord(os, "test", System.currentTimeMillis());
      
      long testAttemptTime = System.currentTimeMillis();
      
      NIOHttpHeaders testHeaders = new NIOHttpHeaders();
      testHeaders.add("test", "test-value");
      
      for (TestRecord record : recordSet) { 
        long preWritePos = os.getPos();
        ArcFileReaderTests.write(os,record.url,"test",1,1,record.data,0,record.data.length,testHeaders,"text/html",MD5Hash.digest(record.data).toString(),12345,testAttemptTime);
        long postWritePos = os.getPos();
        record.streamPos = (int)preWritePos;
        record.rawSize = (int) (postWritePos - preWritePos);
      }
      os.flush();
    }
    finally { 
      os.close();
    }
    return new Pair<Path,List<TestRecord>>(filePath,recordSet);
  }
  
  List<Pair<Path,List<TestRecord>>> buildTestFiles(Path rootPath,FileSystem fs,int numOfTestFiles)throws IOException { 
    List<Pair<Path,List<TestRecord>>> list = Lists.newArrayList();
    for (int i=0;i<numOfTestFiles;++i) { 
      list.add(buildTestARCFile(rootPath, fs, i));
    }
    return list;
  }
  
  static final int NUM_ITERATIONS = 1;
  static final int NUM_TEST_FILES = 10;

  static final int getIndexOfSplit(List<Pair<Path,List<TestRecord>>> splits, InputSplit targetSplit) { 
    for (int i=0;i<splits.size();++i) { 
      Path pathAtIndex = splits.get(i).e0;
      if (((FileSplit)targetSplit).getPath().getName().equals(pathAtIndex.getName())) { 
        return i;
      }
    }
    return -1;
  }
  
  static void validateSplit(FileSystem fs,InputSplit split,List<Pair<Path,List<TestRecord>>> splits,RecordReader<Text,BytesWritable> reader) throws IOException, InterruptedException {
    
    int splitDataIndex = getIndexOfSplit(splits,split);
    
    Assert.assertTrue(splitDataIndex != -1);
    
    List<TestRecord> records = splits.get(splitDataIndex).e1;
    
    int itemIndex = 0;
    // iterate and validate stuff ... 
    while (reader.nextKeyValue()) {
      Text key = reader.getCurrentKey();
      BytesWritable value = reader.getCurrentValue();
      
      TestRecord testRecord = records.get(itemIndex++);
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
    
    Assert.assertEquals(itemIndex,ArcFileReaderTests.BASIC_TEST_RECORD_COUNT);
    
    splits.remove(splitDataIndex);
    
  }
  
  static void validateArcFileItemSplit(FileSystem fs,InputSplit split,List<Pair<Path,List<TestRecord>>> splits,RecordReader<Text,ArcFileItem> reader) throws IOException, InterruptedException {
    
    int splitDataIndex = getIndexOfSplit(splits,split);
    
    Assert.assertTrue(splitDataIndex != -1);
    
    List<TestRecord> records = splits.get(splitDataIndex).e1;
    
    int itemIndex = 0;
    // iterate and validate stuff ...
    while (reader.nextKeyValue()) {
      
      Text key = reader.getCurrentKey();
      ArcFileItem value = reader.getCurrentValue();
      
      TestRecord testRecord = records.get(itemIndex++);
      
      // get test key bytes as utf-8 bytes ... 
      byte[] testKeyBytes = testRecord.url.getBytes(Charset.forName("UTF-8"));
      // compare against raw key bytes to validate key is the same (Text's utf-8 mapping code replaces invalid characters 
      // with ?, which causes our test case (which does use invalid characters to from the key, to break.
      Assert.assertTrue(ArcFileReaderTests.compareTo(testKeyBytes,0,testKeyBytes.length,key.getBytes(),0,key.getLength()) == 0);
      // retured bytes represent the header(encoded in utf-8), terminated by a \r\n\r\n. The content follows this terminator
      // we search for this specific byte pattern to locate start of content, then compare it against source ... 
      Assert.assertTrue(ArcFileReaderTests.compareTo(testRecord.data,0,testRecord.data.length,value.getContent().getReadOnlyBytes(),value.getContent().getOffset(),value.getContent().getCount()) == 0);
      NIOHttpHeaders headers = ArcFileItemUtils.buildHeaderFromArcFileItemHeaders(value.getHeaderItems());
      // validate metadata 
      Assert.assertEquals("text/html",headers.findValue(Constants.ARCFileHeader_ARC_MimeType));
      Assert.assertEquals(value.getArcFilePos(),testRecord.streamPos);
      Assert.assertEquals(value.getArcFileSize(),testRecord.rawSize);
      Assert.assertEquals("test-value", headers.findValue("test"));
      Assert.assertEquals(value.getArcFileName(),((FileSplit)split).getPath().getName());
      
    }
    reader.close();
    
    Assert.assertEquals(itemIndex,ArcFileReaderTests.BASIC_TEST_RECORD_COUNT);
    
    splits.remove(splitDataIndex);
    
  }
  
  @Test
  public void TestArcInputFormat() throws IOException, InterruptedException {
    for (int i=0;i<NUM_ITERATIONS;++i) { 
      Job job = new Job();
      FileSystem fs = LocalFileSystem.get(job.getConfiguration());
      Path path = new Path("/tmp/" + File.createTempFile("ARCInputFormat", "test").getName());
      fs.mkdirs(path);
      
      List<Pair<Path,List<TestRecord>>> fileList = buildTestFiles(path, fs, NUM_TEST_FILES);
      
      FileInputFormat.setInputPaths(job, path);
      
      ARCFileInputFormat inputFormat = new ARCFileInputFormat();
      
      List<InputSplit> splits = inputFormat.getSplits(job);
      
      for (InputSplit split : splits) { 
        RecordReader<Text,BytesWritable> reader = inputFormat.createRecordReader(split, new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID()));
        validateSplit(fs,split,fileList,reader);
      }
      
      Assert.assertTrue(fileList.size() == 0);
      
      fs.delete(path, true);
    }
    
  }

  @Test
  public void TestArcFileItemInputFormat() throws IOException, InterruptedException {
    for (int i=0;i<NUM_ITERATIONS;++i) { 
      Job job = new Job();
      FileSystem fs = LocalFileSystem.get(job.getConfiguration());
      Path path = new Path("/tmp/" + File.createTempFile("ARCInputFormat", "test").getName());
      fs.mkdirs(path);
      
      List<Pair<Path,List<TestRecord>>> fileList = buildTestFiles(path, fs, NUM_TEST_FILES);
      
      FileInputFormat.setInputPaths(job, path);
      
      ARCFileItemInputFormat inputFormat = new ARCFileItemInputFormat();
      
      List<InputSplit> splits = inputFormat.getSplits(job);
      
      for (InputSplit split : splits) { 
        RecordReader<Text,ArcFileItem> reader = inputFormat.createRecordReader(split, new TaskAttemptContext(job.getConfiguration(), new TaskAttemptID()));
        validateArcFileItemSplit(fs, split, fileList, reader);
      }
      
      Assert.assertTrue(fileList.size() == 0);
      
      fs.delete(path, true);
    }
    
  }
  
}
