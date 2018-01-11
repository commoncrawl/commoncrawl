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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/** 
 * Consume ARC Files from a Hadoop FileSystem (local,hdfs,s3n etc.) 
 * 
 * @author rana
 *
 */
public class ARCFileInputFormat extends FileInputFormat<Text, BytesWritable> {

  static final String ARC_SUFFIX = "arc.gz";
  
  /** 
   * use this flag to use S3InputStream instead of the S3NFileSystem provided InputStream 
   * to download S3 content. The (non-emr) version of the S3N unfortunately uses an outdated
   * version of the jets3t libraries and performs very poorly with regards to Network IO. 
   *   
   */
  public static final String USE_S3_INPUTSTREAM = "fs.s3n.use.s3inputstream";
  
    
	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split,TaskAttemptContext context) throws IOException,InterruptedException {
	  ARCFileRecordReader reader = new ARCFileRecordReader();
	  reader.initialize(split, context);
	  return reader;
	}
	
	/** 
	 * we only want to process arc files
	 */
	@Override
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
	  // delegate to super class 
	  ArrayList<FileStatus> candidateList = (ArrayList<FileStatus>) super.listStatus(job);
	  // walk list removing invalid entries
	  for (int i=0;i<candidateList.size();++i) { 
	    Path pathAtIndex = candidateList.get(i).getPath();
	    if (!pathAtIndex.getName().endsWith(ARC_SUFFIX)) { 
	      // remove invalid entry 
	      candidateList.remove(i);
	      // decerement iterator
	      --i;
	    }
	  }
	  return candidateList;
	}

	/**
	 * we don't allow splitting of ARC Files
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

}
