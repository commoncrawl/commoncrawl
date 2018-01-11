package org.commoncrawl.hadoop.io.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.ArcFileItemUtils;

/** 
 * An InputFormat for java based projects that returns the ARC payload in 
 * a convenient ArcFileItem data structure. 
 * 
 * @author rana
 *
 */
public class ARCFileItemInputFormat extends FileInputFormat<Text, ArcFileItem>{

  @Override
  public RecordReader<Text, ArcFileItem> getRecordReader(final InputSplit split,JobConf job, Reporter reporter) throws IOException {
    final ARCFileRecordReader reader = new ARCFileRecordReader();
    reader.initialize(job, split);
    return new RecordReader<Text, ArcFileItem>() {

      BytesWritable valueBytes = new BytesWritable();
      
      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public Text createKey() {
        return new Text();
      }

      @Override
      public ArcFileItem createValue() {
        // TODO Auto-generated method stub
        return new ArcFileItem();
      }

      @Override
      public long getPos() throws IOException {
        return reader.getPos();
      }

      @Override
      public float getProgress() throws IOException {
        return reader.getProgress();
      }

      @Override
      public boolean next(Text key, ArcFileItem value) throws IOException {
        long preReadPos = reader.reader.getPosition();
        if (reader.next(key, valueBytes)) { 
          long postReadPos = reader.reader.getPosition();
          // extract item from raw bytes writable 
          ArcFileItemUtils.bytesWritableToArcFileItem(key, valueBytes, value);
          // set up arc file related fields 
          value.setArcFileName(((FileSplit)split).getPath().getName());
          value.setArcFilePos((int)preReadPos);
          value.setArcFileSize((int)(postReadPos-preReadPos));
          
          return true;
        }
        return false;
      }
    };
  }
  
  /** 
   * we only want to process arc files
   */
  @Override
  protected FileStatus[] listStatus(JobConf job) throws IOException {
    return ARCFileInputFormat.filterInputCandidates(super.listStatus(job));
  }
  
  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }

  

}
