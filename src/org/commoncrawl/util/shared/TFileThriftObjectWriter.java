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
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

/** encapsulates various components necessary to write a TFile
 * 
 * @author rana
 *
 */
@SuppressWarnings("unchecked")
public class TFileThriftObjectWriter<KeyType extends TBase,ValueType extends TBase> {

  
  static final Log LOG = LogFactory.getLog(TFileWriter.class);
  
  FSDataOutputStream _stream;
  TFile.Writer       _writer;
  DataOutputBuffer   _keyStream = new DataOutputBuffer();
  DataOutputBuffer   _valueStream = new DataOutputBuffer();
  TBinaryProtocol    _keyProto = new TBinaryProtocol(new TIOStreamTransport(_keyStream));
  TBinaryProtocol    _valueProto = new TBinaryProtocol(new TIOStreamTransport(_valueStream));
  
  public TFileThriftObjectWriter(FileSystem fs,Configuration conf,Path path,String compressionScheme,int minCompressedBlockSize,Class< ? extends RawComparator<KeyType>> comparatorClass) throws IOException { 
    _stream = fs.create(path);
    _writer = new TFile.Writer(_stream,minCompressedBlockSize,compressionScheme,TFile.COMPARATOR_JCLASS + comparatorClass.getName(), conf);
  }
  
  public void append(KeyType key,ValueType value) throws IOException { 
    try { 
      _keyStream.reset();
      key.write(_keyProto);
      _valueStream.reset();
      value.write(_valueProto);
    }
    catch (TException e) { 
      throw new IOException(e);
    }
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
