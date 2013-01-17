package org.commoncrawl.hadoop.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/** 
 * Consume ARC Files via S3N FileSystem on EMR 
 * 
 * @author rana
 *
 */
public class ARCFileInputFormat extends FileInputFormat<Text, BytesWritable> {

  private static final String ARC_SUFFIX = "arc.gz";
    
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
