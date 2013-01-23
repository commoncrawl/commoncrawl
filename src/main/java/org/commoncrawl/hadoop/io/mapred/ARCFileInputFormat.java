package org.commoncrawl.hadoop.io.mapred;

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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/** 
 *
 * Consume ARC Files from a Hadoop FileSystem (local,hdfs,s3n etc.)
 * 
 * @author rana
 *
 */
public class ARCFileInputFormat extends FileInputFormat<Text, BytesWritable> {

  private static final String ARC_SUFFIX = "arc.gz";

  @Override
  public RecordReader<Text, BytesWritable> getRecordReader(InputSplit split,JobConf job, Reporter reporter) throws IOException {
    ARCFileRecordReader reader = new ARCFileRecordReader();
    reader.initialize(job, split);
    return reader;
  }

  /** 
   * we only want to process arc files
   */
  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    // delegate to super class 
    FileStatus candidateList[] = super.listStatus(job);
    // allocate new array 
    ArrayList<FileStatus> listOut = new ArrayList<FileStatus>(candidateList.length);
    // walk list removing invalid entries
    for (int i=0;i<candidateList.length;++i) { 
      Path pathAtIndex = candidateList[i].getPath();
      if (pathAtIndex.getName().endsWith(ARC_SUFFIX)) { 
        // add to final list
        listOut.add(candidateList[i]);
      }
    }
    return listOut.toArray(new FileStatus[listOut.size()]);
  }

  /**
   * we don't allow splitting of ARC Files
   */
  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }
}
