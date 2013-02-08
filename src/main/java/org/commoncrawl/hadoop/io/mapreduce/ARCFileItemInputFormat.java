package org.commoncrawl.hadoop.io.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.ArcFileItemUtils;

public class ARCFileItemInputFormat extends FileInputFormat<Text, ArcFileItem>{

  @Override
  public RecordReader<Text, ArcFileItem> createRecordReader(final InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {

    final ARCFileRecordReader reader = new ARCFileRecordReader();
    reader.initialize(split, context);
    
    return new RecordReader<Text, ArcFileItem>() {

      final ArcFileItem currentValue = new ArcFileItem();
      
      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public Text getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
      }

      @Override
      public ArcFileItem getCurrentValue() throws IOException,
          InterruptedException {
        return currentValue;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)throws IOException, InterruptedException {
        
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        long preReadPos = reader.reader.getPosition();
        if (reader.nextKeyValue()) { 
          long postReadPos = reader.reader.getPosition();
         
          // extract item from raw bytes writable 
          ArcFileItemUtils.bytesWritableToArcFileItem(reader.getCurrentKey(), reader.getCurrentValue(), currentValue);
          // set up arc file related fields 
          currentValue.setArcFileName(((FileSplit)split).getPath().getName());
          currentValue.setArcFilePos((int)preReadPos);
          currentValue.setArcFileSize((int)(postReadPos-preReadPos));
          
          return true;
        }
        return false;
      }
    };
  }
  
  /** 
   * we only want to process arc files
   * 
   */
  //TODO: CONSOLIDATE DUPLICATE CODE 
  @Override
  protected List<FileStatus> listStatus(JobContext job) throws IOException {
    // delegate to super class 
    ArrayList<FileStatus> candidateList = (ArrayList<FileStatus>) super.listStatus(job);
    // walk list removing invalid entries
    for (int i=0;i<candidateList.size();++i) { 
      Path pathAtIndex = candidateList.get(i).getPath();
      if (!pathAtIndex.getName().endsWith(ARCFileInputFormat.ARC_SUFFIX)) { 
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
