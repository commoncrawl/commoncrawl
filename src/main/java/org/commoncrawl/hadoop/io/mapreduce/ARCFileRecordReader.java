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
 */


import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.commoncrawl.util.shared.ARCFileReader;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.S3InputStream;
import org.jets3t.service.Jets3tProperties;

/** 
 * Reads an ARCFile whose location is spcified via a FileInputSplit
 * 
 * @author rana
 *
 */
public class ARCFileRecordReader extends RecordReader<Text, BytesWritable>{

  
  /** logging **/
  private static final Log LOG = LogFactory.getLog(ARCFileRecordReader.class);

  protected Configuration conf;
  ARCFileReader reader;
  private Text key = new Text();
  private BytesWritable value = new BytesWritable();
  private long start;
  private long end;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    conf = context.getConfiguration();    
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    InputStream in = null;
    if (fs instanceof NativeS3FileSystem) {
      if (conf.getBoolean(ARCFileInputFormat.USE_S3_INPUTSTREAM, false)) {
        in = new S3InputStream(path.toUri(), conf.get("fs.s3n.awsAccessKeyId"), conf.get("fs.s3n.awsSecretAccessKey"), 1048576);
      }
      else { 
        Jets3tProperties properties = Jets3tProperties.getInstance(org.jets3t.service.Constants.JETS3T_PROPERTIES_FILENAME);
        properties.setProperty("s3service.https-only","false");
      }
    }
    if (in == null) { 
      in = fs.open(path);
    }
    try { 
      reader = new ARCFileReader(in);
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      in.close();
      throw e;
    }
    start = fileSplit.getStart();
    end   = fileSplit.getLength();
    if (start != 0 || fs.getFileStatus(path).getLen() != end) { 
      throw new IOException("Invalid FileSplit encountered! Split Details:" + split.toString());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (reader.hasMoreItems()) { 
      reader.nextKeyValue(key,value);
      return true;
    }
    return false;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return value; 
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return Math.min(1.0f, (reader.getPosition() - start) / (float)(end - start));
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
