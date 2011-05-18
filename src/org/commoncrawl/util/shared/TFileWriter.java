package org.commoncrawl.util.shared;

import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.RawComparable;
import org.apache.hadoop.io.file.tfile.TFile;

/** encapsulates various components necessary to write a TFile
 * 
 * @author rana
 *
 */
public class TFileWriter<KeyType extends Writable,ValueType extends Writable> {

  
  static final Log LOG = LogFactory.getLog(TFileWriter.class);
  
  FSDataOutputStream _stream;
  TFile.Writer       _writer;
  DataOutputBuffer   _keyStream = new DataOutputBuffer();
  DataOutputBuffer   _valueStream = new DataOutputBuffer();
  
  public TFileWriter(FileSystem fs,Configuration conf,Path path,String compressionScheme,int minCompressedBlockSize,Class< ? extends RawComparator<KeyType>> comparatorClass) throws IOException { 
    _stream = fs.create(path);
    _writer = new TFile.Writer(_stream,minCompressedBlockSize,compressionScheme,TFile.COMPARATOR_JCLASS + comparatorClass.getName(), conf);
  }
  
  public void append(KeyType key,ValueType value) throws IOException { 
    _keyStream.reset();
    key.write(_keyStream);
    _valueStream.reset();
    value.write(_valueStream);
    append(_keyStream,_valueStream);
  }
  
  public final void append(DataOutputBuffer keyStream,DataOutputBuffer valueStream) throws IOException { 
    _writer.append(_keyStream.getData(),0,_keyStream.getLength(),_valueStream.getData(),0,_valueStream.getLength());
  }
  
  public TFile.Writer getUnderlyingWriter() { 
    return _writer;
  }
  
  public long getPosition() throws IOException { 
    return _stream.getPos();
  }
  
  public void close() { 
    if (_writer != null) { 
      try {
        _writer.close();
      } catch (Exception e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      finally { 
        _writer = null;
      }
    }
    
    if (_stream != null) { 
      try {
        _stream.close();
      } catch (Exception e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      finally { 
        _stream = null;
      }
    }
  }
  
}
