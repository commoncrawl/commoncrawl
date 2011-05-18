package org.commoncrawl.util.shared;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 * encapsulate all the elements necessary to read a TFile
 * 
 * @author rana
 *
 */
public class TFileThriftObjectReader<KeyType extends TBase,ValueType extends TBase> {
  
  @SuppressWarnings("unused")
  public static class TResettableTransport extends TIOStreamTransport { 
    
    public void setInputStream(InputStream is) { 
      this.inputStream_ = is;
    }
    
    public void setOutputStream(OutputStream os) { 
      this.outputStream_ = os;
    }
  }
  
  static final Log LOG = LogFactory.getLog(TFileWritableReader.class);
      
  private FSDataInputStream     _inputStream;
  private TFile.Reader          _reader;
  private boolean               _ownsReader;
  private TFile.Reader.Scanner  _scanner;
  private TResettableTransport  _keyTransport = new TResettableTransport();
  private TResettableTransport  _valueTransport = new TResettableTransport();
  private TBinaryProtocol       _keyProtocol= new TBinaryProtocol(_keyTransport);
  private TBinaryProtocol       _valueProtocol = new TBinaryProtocol(_valueTransport);
  
  
  
  public TFileThriftObjectReader(TFile.Reader reader) throws IOException { 
    if (reader == null) { 
      throw new IOException("Reader == null!");
    }
    _reader = reader;
    _ownsReader = false;
    _inputStream = null;
    _scanner = _reader.createScanner();
  }
  /**
   * Construct a reader object given the path to the underlying TFile
   * 
   * @param fs
   * @param conf
   * @param path
   * @throws IOException
   */
  public TFileThriftObjectReader(FileSystem fs,Configuration conf,Path path) throws IOException { 
    FileStatus fileStatus = fs.getFileStatus(path);
    if (fileStatus == null) { 
      throw new IOException("Invalid File:" + path);
    }
    
    _ownsReader = true;
    _inputStream = fs.open(path);
    _reader = new TFile.Reader(_inputStream,fileStatus.getLen(),conf);
    _scanner = _reader.createScanner();
  }
  
  public TFile.Reader getReader() { 
    return _reader;
  }
  
  public TFile.Reader.Scanner getScanner() { 
    return _scanner;
  }
  
  public boolean next(KeyType keyType,ValueType valueType) throws IOException { 
    if (_scanner != null && !_scanner.atEnd()) {
      try {
        _keyTransport.setInputStream(_scanner.entry().getKeyStream());
        keyType.read(_keyProtocol);
        if (valueType != null) { 
          _valueTransport.setInputStream(_scanner.entry().getValueStream());
          valueType.read(_valueProtocol);
        }
      } catch (TException e) {
        throw new IOException(e);
      }
      
      _scanner.advance();
      
      return true;
    }
    return false;
  }
  
  public boolean hasMoreData() { 
    return (_scanner != null && !_scanner.atEnd());
  }
  
  public void close() { 
    if (_scanner != null)
      try {
        _scanner.close();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      finally { 
        _scanner = null;
      }
      
    if (_reader != null && _ownsReader)
      try {
        _reader.close();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
      finally { 
        _reader = null;
      }
      
    if (_inputStream != null)
      try {
        _inputStream.close();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));      
      }
      finally { 
        _inputStream = null;
      }
      
  }

}
