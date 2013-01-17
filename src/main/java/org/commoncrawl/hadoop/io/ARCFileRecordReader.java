package org.commoncrawl.hadoop.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.commoncrawl.protocol.shared.ArcFileItem;

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
  protected FSDataInputStream in;
  protected ARCFileReader reader;
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
    in = fs.open(path);
    reader = new ARCFileReader(in);
    start = fileSplit.getStart();
    end   = fileSplit.getLength();
    if (start != 0 || fs.getFileStatus(path).getLen() != end) { 
      throw new IOException("Invalid FileSplit encountered! Split Details:" + split.toString());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (reader.hasMoreItems()) { 
      reader.getNextItem(key,value);
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
    return Math.min(1.0f, (in.getPos() - start) / (float)(end - start));
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
