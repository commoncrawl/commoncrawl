package org.commoncrawl.util.shared;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.TFile;

/**
 * encapsulate all the elements necessary to read a TFile
 * 
 * @author rana
 *
 */
public class TFileReader<KeyType extends Writable,ValueType extends Writable> {
  
  static final Log LOG = LogFactory.getLog(TFileReader.class);
      
  private FSDataInputStream     _inputStream;
  private TFile.Reader          _reader;
  private TFile.Reader.Scanner  _scanner;
  
  public TFileReader(FileSystem fs,Configuration conf,Path path) throws IOException { 
    FileStatus fileStatus = fs.getFileStatus(path);
    if (fileStatus == null) { 
      throw new IOException("Invalid File:" + path);
    }
    
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
      keyType.readFields(_scanner.entry().getKeyStream());
      valueType.readFields(_scanner.entry().getValueStream());
      
      _scanner.advance();
      
      return true;
    }
    return false;
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
      
    if (_reader != null)
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
