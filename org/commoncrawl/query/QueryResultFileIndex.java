package org.commoncrawl.query;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.commoncrawl.hadoop.mergeutils.MergeSortSpillWriter;
import org.commoncrawl.hadoop.mergeutils.SequenceFileIndexWriter;
import org.commoncrawl.query.ClientQueryInfo;
import org.commoncrawl.query.ClientQueryInfo.SortOrder;
import org.commoncrawl.util.shared.CCStringUtils;

/**
 * Creates an index into a sequence file
 * 
 * @author rana
 *
 * @param <KeyType>
 * @param <ValueType>
 */
public class QueryResultFileIndex<KeyType extends WritableComparable,ValueType extends Writable> {
  
  private static final Class[] emptyArray = new Class[]{};
  
  FileSystem _fileSystem;
  Path _indexFileName;
  PositionBasedIndexWriter.IndexHeader _header = new PositionBasedIndexWriter.IndexHeader();
  ByteBuffer _indexData = null;
  DataInputStream _inputStream = null;
  int _headerOffset = -1;
  int _indexItemCount;
  static final int INDEX_RECORD_SIZE = 16;
  
  Constructor<KeyType> keyConstructor = null;
  Constructor<ValueType> valConstructor = null;

  
  
  public static final Log LOG = LogFactory.getLog(QueryResultFileIndex.class);
  
  
  public static Path getIndexNameFromBaseName(Path baseFileName) { 
    return new Path(baseFileName.getParent(), baseFileName.getName() + ".index");
  }

  public static Path getBaseNameFromIndexName(Path indexName) { 
    //LOG.info("Index Name is:" + indexName.getName());
    String baseName = indexName.getName().substring(0,indexName.getName().length() - ".index".length());
    //LOG.info("Base Name is:" + baseName);

    return new Path(indexName.getParent(),baseName );
  }
  
  public QueryResultFileIndex(
        FileSystem fileSystem,
      Path indexFilePath,
      Class<KeyType> keyClass,
      Class<ValueType> valueClass
      )throws IOException {
    
    _fileSystem = fileSystem; 
    _indexFileName = indexFilePath;
    
    if (!_fileSystem.exists(_indexFileName) || _fileSystem.isDirectory(_indexFileName)) {
      throw new IOException("Index Path:" + indexFilePath + " Points to Invalid File");
    }
    else { 
      
      try {
        this.keyConstructor = keyClass.getDeclaredConstructor(emptyArray);
        this.keyConstructor.setAccessible(true);
        this.valConstructor = valueClass.getDeclaredConstructor(emptyArray);
        this.valConstructor.setAccessible(true);
      } catch (SecurityException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
      
      _indexData = loadStreamIntoMemory(indexFilePath);
      _inputStream = new DataInputStream(newInputStream(_indexData));
      _header.readHeader(_inputStream);
      _headerOffset = _indexData.position();
      // calculate index item count based on file size 
      _indexItemCount = (int) (_indexData.remaining() / INDEX_RECORD_SIZE); 
    }
  }
  
  
  public long getRecordCount() { return _header._totalRecordCount; }
  
  private static InputStream newInputStream(final ByteBuffer buf) {
    return new InputStream() {
        public synchronized int read() throws IOException {
            if (!buf.hasRemaining()) {
                LOG.error("EOF REACHED in Wrapper Stream!");
                return -1;
            }
            return buf.get() & 0xff;
        }

        public synchronized int read(byte[] bytes, int off, int len) throws IOException {
            // Read only what's left
            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }
    };
  }
  
  private static class IndexItem {
    
    public IndexItem(long indexValue,long offsetValue) { 
      _indexValue = indexValue;
      _offsetValue = offsetValue;
    }
    long _indexValue;
    long _offsetValue;
  }
  
  private IndexItem findIndexDataPosForItemIndex(long targetItemIndexValue)throws IOException {
    
    int low = 0;
    int high = _indexItemCount - 1;
    while (low <= high) {
      int mid = low + ((high - low) / 2);
      _indexData.position(_headerOffset + (mid * (INDEX_RECORD_SIZE)));
      long indexValue =_inputStream.readLong();
      int comparisonResult = (int)(indexValue - targetItemIndexValue);
      if (comparisonResult > 0)
          high = mid - 1;
      else if (comparisonResult < 0)
          low = mid + 1;
      else {
          return new IndexItem(indexValue,_inputStream.readLong()); // found
      }
    }
    if (high == -1)
      return null;
    else {
      _indexData.position(_headerOffset + (high * (INDEX_RECORD_SIZE)));
      return new IndexItem(_inputStream.readLong(),_inputStream.readLong()); // not found
    }
  }
  
  public void dump() throws IOException {
    //LOG.info("Record Count:"+ this._header._totalRecordCount);
    
    for (long i=0;i < _header._totalRecordCount;i+= 100) {
      IndexItem itemData = findIndexDataPosForItemIndex(i);
      //LOG.info("Pos for Item:" + i + " is:[" + itemData._indexValue + "," + itemData._offsetValue +"]" );
    }
  }
  
  public void seekReaderToItemAtIndex(SequenceFile.Reader reader, long desiredIndexPos)throws IOException {
    IndexItem indexItem =findIndexDataPosForItemIndex(desiredIndexPos);
    if (indexItem == null) { 
      throw new IOException("Invalid Index Position:" + desiredIndexPos );
    }
      
    //LOG.info("Seeking to appropriate position in file");
    long timeStart = System.currentTimeMillis();
    reader.seek(indexItem._offsetValue);
    //LOG.info("Seek Took:" + (System.currentTimeMillis() - timeStart));
    
    DataOutputBuffer skipBuffer = new DataOutputBuffer() { 
      @Override
      public void write(DataInput in, int length) throws IOException {
        in.skipBytes(length);
      }      
    };
    
    timeStart = System.currentTimeMillis();
    
    int skipCount = 0;
    
    ValueBytes skipValue = reader.createValueBytes();    
    
    long currentIndexPos = indexItem._indexValue;
    while (currentIndexPos < desiredIndexPos) { 
      
      reader.nextRawKey(skipBuffer);
      reader.nextRawValue(skipValue);
      ++skipCount;
      ++currentIndexPos;
    }
    
    //LOG.info("Skip of:" + skipCount +" Values took:" + (System.currentTimeMillis() - timeStart));
    
  }
  
  /**
   * read paginated results from the underlying sequence file 
   * @param fileSystem
   * @param conf
   * @param sortOrder
   * @param pageNumber
   * @param pageSize
   * @param resultOut
   * @throws IOException
   */
  public void readPaginatedResults(FileSystem fileSystem,Configuration conf,int sortOrder,int pageNumber,int pageSize,QueryResult<KeyType,ValueType> resultOut)throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem,getBaseNameFromIndexName(_indexFileName),conf);
    
    try { 
      readPaginatedResults(reader,sortOrder,pageNumber,pageSize,resultOut);
    }
    finally { 
      reader.close();
    }
    
  }
  
  public void readPaginatedResults(SequenceFile.Reader reader,int sortOrder,int pageNumber,int pageSize,QueryResult<KeyType,ValueType> resultOut)throws IOException { 
    // if descending sort order ... 
    // take pageNumber * pageSize as starting point
    long offset = 0;
    long startPos = 0;
    long endPos   = 0;
    
    resultOut.getResults().clear();
    resultOut.setPageNumber(pageNumber);
    resultOut.setTotalRecordCount(_header._totalRecordCount);
        
    
    if (sortOrder == ClientQueryInfo.SortOrder.ASCENDING) { 
      startPos = pageNumber * pageSize;
      endPos   = Math.min(startPos + pageSize, _header._totalRecordCount);
      offset = pageNumber * pageSize;
    }
    else { 
      startPos = _header._totalRecordCount - ((pageNumber +1) * pageSize);
      endPos   = startPos + pageSize;
      startPos = Math.max(0,startPos);
      offset = _header._totalRecordCount - ((pageNumber +1) * pageSize);
    }
    LOG.info("readPaginatedResults called on Index with sortOrder:" + sortOrder + " pageNumber: " + pageNumber + " pageSize:" + pageSize + " offset is:" + offset);
    if (startPos < _header._totalRecordCount) { 
      
      //LOG.info("Seeking to Offset:" + startPos);
      seekReaderToItemAtIndex(reader,startPos);
      //LOG.info("Reading from:"+ startPos + " to:" + endPos + " (exclusive)");
      for (long i=startPos;i<endPos;++i) {
        KeyType key = null;
        ValueType value = null;
        try {
          key   = keyConstructor.newInstance();
          value = valConstructor.newInstance();
        } catch (Exception e) {
          LOG.error("Failed to create key or value type with Exception:" + CCStringUtils.stringifyException(e));
          throw new RuntimeException(e);
        }
        
        if (reader.next(key, value)) {
          if (sortOrder == ClientQueryInfo.SortOrder.DESCENDING) { 
            resultOut.getResults().add(0,new QueryResultRecord<KeyType,ValueType>(key,value));
          }
          else { 
            resultOut.getResults().add(new QueryResultRecord<KeyType,ValueType>(key,value));
          }
        }
        else { 
          break;
        }
      }
    }
  }
  
  private ByteBuffer loadStreamIntoMemory(Path streamPath)throws IOException { 
    //LOG.info("Loading Stream:" + streamPath.getAbsolutePath());
    if (!_fileSystem.exists(streamPath) || _fileSystem.isDirectory(streamPath)) {
      throw new IOException("Stream Path:" + streamPath + " Points to Invalid File");
    }
    else { 
      DataInputStream inputStream = null;
      ByteBuffer bufferOut = null; 
      try { 
        
        LOG.info("Allocating Buffer of size:" + _fileSystem.getLength(streamPath) + " for Stream:" + streamPath);
        bufferOut = ByteBuffer.allocate((int) _fileSystem.getLength(streamPath));
        inputStream = _fileSystem.open(streamPath);
        long loadStart = System.currentTimeMillis();
        for (int offset=0,totalRead=0;offset<bufferOut.capacity();) { 
          int bytesToRead = Math.min(16384,bufferOut.capacity() - totalRead);
          inputStream.read(bufferOut.array(),offset,bytesToRead);
          offset+= bytesToRead;
          totalRead += bytesToRead;
        }
        //LOG.info("Load of Stream:" + streamPath.getAbsolutePath() + " Took:" + (System.currentTimeMillis() - loadStart) + " MS");
      } 
      finally { 
        if (inputStream != null) { 
          inputStream.close();
        }
      }
      
      return bufferOut;
    }
  }
  
  @SuppressWarnings("unchecked")
  public static class PositionBasedIndexWriter implements SequenceFileIndexWriter{
    
    
    public static final Log LOG = LogFactory.getLog(MergeSortSpillWriter.class);
    
    private FileSystem           _fileSystem;
    private Path             _indexFileName;
    private RandomAccessFile _indexFile = null;
    private File                         _tempFileName;
    private IndexHeader _header = null;
    public long  _lastKnownStartIndex = -1;
    public long  _lastKnownFileLength = -1;
    public int   _level1IndexItemCount = 0;

    public static class IndexHeader { 
      
      public short _version = 01;
      public long  _totalRecordCount = 0;
    
      public void readHeader(DataInput stream) throws IOException { 
        _version = stream.readShort();
        _totalRecordCount = stream.readLong();
      }
      public void writeHeader(DataOutput stream) throws IOException { 
        stream.writeShort(_version);
        stream.writeLong(_totalRecordCount);
      }
      
      public static int sizeOfHeader() { 
        return 2+4+8;
      }
    }
    
    public PositionBasedIndexWriter(FileSystem fileSystem,Path indexFilePath)throws IOException {
      _fileSystem = fileSystem;
      _fileSystem.delete(indexFilePath);
      _indexFileName = indexFilePath;
      _tempFileName = File.createTempFile("indexTmp", Long.toString(System.currentTimeMillis()));
      _indexFile = new RandomAccessFile(_tempFileName,"rw");
      
      _header = new IndexHeader();
      
      // write empty header to disk 
      _header.writeHeader(_indexFile);
    }
    
    public Path getPath() { return _indexFileName; }
    
    public void close()throws IOException { 
      if (_indexFile != null) { 
        //LOG.info("Level 1 Index Count:" + _level1IndexItemCount);
        try {
          // reseek to zero 
          _indexFile.seek(0);
          // and rewrite header ...
          _header.writeHeader(_indexFile);
        }
        finally { 
          _indexFile.close();
        }
        _indexFile = null;
        
        // copy across to the remote file system.
        _fileSystem.copyFromLocalFile(new Path(_tempFileName.getAbsolutePath()),_indexFileName);
      }
    }
    
        @Override
    public void indexItem(byte[] keyData, int keyOffset, int keyLength,
        byte[] valueData, int valueOffset, int valueLength, long currentFileLength)throws IOException {

      // check to see if block position changed ... 
      if (currentFileLength != _lastKnownFileLength){
        // establish new start index
        _lastKnownStartIndex = _header._totalRecordCount;
        // and also update last known file position 
        _lastKnownFileLength = currentFileLength;
        // increment index item count
        ++_level1IndexItemCount;
        //LOG.info("Writing Index Record. StartIndex:" + _lastKnownStartIndex +" FilePos:"+ _lastKnownFileLength);
        // time to write out an index record ... 
        _indexFile.writeLong(_lastKnownStartIndex);
        _indexFile.writeLong(_lastKnownFileLength);
      }
      // now update header count ...
      _header._totalRecordCount++;          
    }
    
  }  
  
  
}
