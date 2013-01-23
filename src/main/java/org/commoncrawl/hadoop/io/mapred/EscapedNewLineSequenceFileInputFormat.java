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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileRecordReader;

/**
 * A Text/Text sequence file inputformat that removes carriage returns and escapes tabs and newlines
 * in the record value
 * 
 * @author matpalm
 */
public class EscapedNewLineSequenceFileInputFormat extends SequenceFileInputFormat<Text, Text>{
  
  @Override
  public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException { 
    return new EscapeNewLineRecordReader(job, (FileSplit)split);
  }

  public final class EscapeNewLineRecordReader extends SequenceFileRecordReader<Text, Text> {
        
    public EscapeNewLineRecordReader(Configuration conf, FileSplit split) throws IOException {
      super(conf, split);
    }

    @Override
    protected synchronized void getCurrentValue(Text value) throws IOException {
      // pull in next text 
      super.getCurrentValue(value);
      
      // clean it ready for streaming
      String cleanedup = value.toString().
          replaceAll("\\r", " ").      // remove rogue CRs, streaming gets confused about these
          trim().                      // remove any obvious leading/trialing stuff (including from those CRs)
          replaceAll("\\n", "\\\\n").  // escape all \n s
          replaceAll("\\t", "\\\\t");  // and since streaming uses \t as k/v sep lets escape them too

      // replace value
      value.set(cleanedup);
    }
    
  }
  
}