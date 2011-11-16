package org.commoncrawl.hadoop.mergeutils;

/*
 *    Copyright 2010 - CommonCrawl Foundation
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.ValueBytes;
import org.commoncrawl.util.shared.CCStringUtils;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * 
 * An output sink that writes to a sequence file
 * 
 * @author rana
 * 
 * @param <KeyType>
 * @param <ValueType>
 */
public class SequenceFileSpillWriter<KeyType extends WritableComparable, ValueType extends Writable>
implements RawDataSpillWriter<KeyType, ValueType> {

  private static final Log LOG = LogFactory
  .getLog(SequenceFileSpillWriter.class);
  private static final int BUFFER_QUEUE_CAPACITY = 100;

  private SequenceFileIndexWriter<KeyType, ValueType> _indexWriter = null;
  private SequenceFile.Writer writer = null;
  private long _recordCount = 0;
  private FSDataOutputStream _outputStream;
  private ByteBuffer _activeBuffer = null;
  private Thread _writerThread = null;
  private IOException _writerException = null;
  private int _spillBufferSize = -1;

  // default buffer size
  public static final int DEFAULT_SPILL_BUFFER_SIZE = 1000000;
  // the number of index items we can store in ram
  public static final String SPILL_WRITER_BUFFER_SIZE_PARAM = "commoncrawl.spillwriter.buffer.size";
  // the queue capacity parameter 
  public static final String QUEUE_CAPACITY_PARAM = "commoncrawl.spillwriter.queue.capacity";

  private static class QueuedBufferItem {

    QueuedBufferItem(ByteBuffer buffer) {
      _buffer = buffer;
    }

    public ByteBuffer _buffer;
  }

  private LinkedBlockingQueue<QueuedBufferItem> _bufferQueue;

  public SequenceFileSpillWriter(FileSystem fileSystem, Configuration conf,
      Path outputFilePath, Class<KeyType> keyClass,
      Class<ValueType> valueClass,
      SequenceFileIndexWriter<KeyType, ValueType> optionalIndexWriter,
      CompressionCodec codec,short replicationFactor) throws IOException {

    _bufferQueue = new LinkedBlockingQueue<QueuedBufferItem>(
        conf.getInt(QUEUE_CAPACITY_PARAM,BUFFER_QUEUE_CAPACITY));
    _spillBufferSize = conf.getInt(SPILL_WRITER_BUFFER_SIZE_PARAM,
        DEFAULT_SPILL_BUFFER_SIZE);
    _outputStream = fileSystem.create(outputFilePath,true,10*1024*1024,replicationFactor,fileSystem.getDefaultBlockSize());
    // allocate buffer ...
    _activeBuffer = ByteBuffer.allocate(_spillBufferSize);
    // assign index writer ..
    _indexWriter = optionalIndexWriter;
    
    if (codec != null) {
      writer = SequenceFile.createWriter(conf, _outputStream, keyClass,
          valueClass, CompressionType.BLOCK, codec);
    } else {
      writer = SequenceFile.createWriter(conf, _outputStream, keyClass,
          valueClass, CompressionType.NONE, null);
    }

    _writerThread = new Thread(new Runnable() {

      @Override
      public void run() {
        // LOG.info("Writer Thread Starting");

        while (true) {

          QueuedBufferItem queuedBufferItem = null;

          try {
            queuedBufferItem = _bufferQueue.take();
          } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          if (queuedBufferItem._buffer == null) {
            // LOG.info("Writer Thread received empty buffer item. Exiting");
            return;
          } else {

            ByteBuffer theBuffer = queuedBufferItem._buffer;

            // LOG.info("Writer Thread received item. Limit:" +
            // theBuffer.limit());

            // get byte pointer
            byte[] bufferAsBytes = theBuffer.array();

            int itemsWritten = 0;
            long timeStart = System.currentTimeMillis();

            while (theBuffer.remaining() != 0) {

              // now read in key length
              int keyLen = theBuffer.getInt();
              // mark key position
              int keyPos = theBuffer.position();
              // now skip past key length
              theBuffer.position(keyPos + keyLen);
              // read value length
              int valueLen = theBuffer.getInt();
              // mark value position
              int valuePosition = theBuffer.position();
              // now skip past it ...
              theBuffer.position(valuePosition + valueLen);
              // now write this out to the sequence file ...

              try {
                spillRawRecord2(bufferAsBytes, keyPos, keyLen, bufferAsBytes,
                    valuePosition, valueLen);
              } catch (IOException e) {
                LOG.error("Writer Thread Failed with Error:"
                    + CCStringUtils.stringifyException(e));
                _writerException = e;
                return;
              }
              itemsWritten++;
            }
            // LOG.info("Writer Thread Finished With Buffer. Wrote:"+
            // itemsWritten + " in:" + (System.currentTimeMillis() -
            // timeStart));
          }
        }
      }

    });
    _writerThread.start();
  }

  @Override
  public void close() throws IOException {

    // flush any active buffers ...
    flushActiveBuffer();

    // wait for writer thread to terminate gracefully
    if (_writerThread != null) {
      try {
        _bufferQueue.put(new QueuedBufferItem(null));
        // LOG.info("Waiting for Writer Thread to Die");
        _writerThread.join();
        // LOG.info("Writer Thread Died. Continuing Close");
        _writerThread = null;
      } catch (InterruptedException e) {
      }
    }

    if (writer != null) {
      // close sequence file writer
      writer.close();
      writer = null;
    }

    // and index writer (if open);
    if (_indexWriter != null) {
      _indexWriter.close();
      _indexWriter = null;
    }
    // and finally output stream
    if (_outputStream != null) {
      _outputStream.close();
      _outputStream = null;
    }

    // LOG.info("Closing Spill File. RecordCount:" + _recordCount);
  }

  private static OutputStream newOutputStream(final ByteBuffer buf) {
    return new OutputStream() {

      @Override
      public void write(int b) throws IOException {
        buf.put((byte) (b & 0xff));
      }

      public void write(byte src[], int off, int len) throws IOException {
        buf.put(src, off, len);
      }
    };
  }

  @Override
  public void spillRecord(KeyType key, ValueType value) throws IOException {

    // mark buffer position ...
    int startPosition = -1;
    // LOG.info("Buffer start position:" + startPositon);

    boolean done = false;

    while (!done) {

      if (_activeBuffer == null) {
        allocateActiveBuffer();
      }

      startPosition = _activeBuffer.position();

      DataOutputStream outputStream = new DataOutputStream(
          newOutputStream(_activeBuffer));
      int keySizePos = 0;
      int valueSizePos = 0;
      int keySize = 0;
      int endPosition = 0;
      int valueSize = 0;
      boolean overflow = false;
      try {
        // save key size position
        keySizePos = _activeBuffer.position();
        // LOG.info("keySizePos:" + keySizePos);
        // skip past key length
        _activeBuffer.position(keySizePos + 4);
        // next write key and value
        key.write(outputStream);
        // now save value size position
        valueSizePos = _activeBuffer.position();
        // and calculate key size
        keySize = valueSizePos - keySizePos - 4;
        // reseek back
        _activeBuffer.position(keySizePos);
        // write out real key size
        _activeBuffer.putInt(keySize);
        // skip to value size position + 4
        _activeBuffer.position(valueSizePos + 4);
        // write out actual value
        value.write(outputStream);
        // save end position
        endPosition = _activeBuffer.position();
        // calculate value size
        valueSize = endPosition - valueSizePos - 4;
        // reseek back to value size pos
        _activeBuffer.position(valueSizePos);
        // write value size
        _activeBuffer.putInt(valueSize);
        // seek forward to end position
        _activeBuffer.position(endPosition);

        // LOG.info("keySizePos:" + keySizePos + " keySize:" + keySize +
        // " valueSizePos:" + valueSizePos + " valueSize:" + valueSize);

        done = true;
      }
      // trap for buffer overflow specifically
      catch (IllegalArgumentException e) {
        // LOG.error("IllegalArgumentException detected while writing to Spill Data Buffer. Flushing");
        overflow = true;
      }
      // trap for buffer overflow specifically
      catch (BufferOverflowException e) {
        // LOG.error("Buffer Overflow detected while writing to Spill Data Buffer. Flushing");
        overflow = true;
      }

      if (overflow) {
        // LOG.error("Overflow Condition StartPosition:" + startPosition +
        // " KeySizePos:" + keySizePos + " keySize:" + keySize +
        // " valueSizePos:" + valueSizePos + " valueSize:" + valueSize);

        if (startPosition == 0) {
          throw new IOException("Key + Value Size too Big for SpillBuffer !");
        }
        // reset to start position
        _activeBuffer.position(startPosition);
        // sort and spill
        flushActiveBuffer();
      }

    }

  }

  @Override
  public void spillRawRecord(byte[] keyData, int keyOffset, int keyLength,
      final byte[] valueData, final int valueOffset, final int valueLength)
  throws IOException {

    // mark buffer position ...
    int startPosition = -1;
    // LOG.info("Buffer start position:" + startPositon);

    boolean done = false;

    while (!done) {

      if (_activeBuffer == null) {
        allocateActiveBuffer();
      }

      startPosition = _activeBuffer.position();
      int keySizePos = 0;
      int valueSizePos = 0;
      int keySize = 0;
      int endPosition = 0;
      int valueSize = 0;
      boolean overflow = false;

      try {
        // save key size position
        keySizePos = _activeBuffer.position();
        // LOG.info("keySizePos:" + keySizePos);
        // skip past key length
        _activeBuffer.position(keySizePos + 4);
        // next write key and value
        _activeBuffer.put(keyData, keyOffset, keyLength);
        // now save value size position
        valueSizePos = _activeBuffer.position();
        // and calculate key size
        keySize = valueSizePos - keySizePos - 4;
        // reseek back
        _activeBuffer.position(keySizePos);
        // write out real key size
        _activeBuffer.putInt(keySize);
        // skip to value size position + 4
        _activeBuffer.position(valueSizePos + 4);
        // write out actual value
        _activeBuffer.put(valueData, valueOffset, valueLength);
        // save end position
        endPosition = _activeBuffer.position();
        // calculate value size
        valueSize = endPosition - valueSizePos - 4;
        // reseek back to value size pos
        _activeBuffer.position(valueSizePos);
        // write value size
        _activeBuffer.putInt(valueSize);
        // seek forward to end position
        _activeBuffer.position(endPosition);

        // LOG.info("keySizePos:" + keySizePos + " keySize:" + keySize +
        // " valueSizePos:" + valueSizePos + " valueSize:" + valueSize);

        done = true;
      }
      // trap for buffer overflow specifically
      catch (IllegalArgumentException e) {
        // LOG.error("IllegalArgumentException detected while writing to Spill Data Buffer. Flushing");
        overflow = true;
      }
      // trap for buffer overflow specifically
      catch (BufferOverflowException e) {
        // LOG.error("Buffer Overflow detected while writing to Spill Data Buffer. Flushing");
        overflow = true;
      }

      if (overflow) {
        // LOG.error("Overflow Condition StartPosition:" + startPosition +
        // " KeySizePos:" + keySizePos + " keySize:" + keySize +
        // " valueSizePos:" + valueSizePos + " valueSize:" + valueSize);

        if (startPosition == 0) {
          throw new IOException("Key + Value Size too Big for SpillBuffer !");
        }
        // reset to start position
        _activeBuffer.position(startPosition);
        // sort and spill
        flushActiveBuffer();
      }
    }
  }

  private void allocateActiveBuffer() throws IOException {
    // LOG.info("Allocating Buffer of Size:" + BUFFER_SIZE);
    _activeBuffer = ByteBuffer.allocate(_spillBufferSize);
  }

  private void flushActiveBuffer() throws IOException {
    try {
      if (_activeBuffer != null && _activeBuffer.position() != 0) {
        // flip the buffer ...
        _activeBuffer.flip();
        // LOG.info("Flipped Active Buffer.Limit:" + _activeBuffer.limit()+
        // ". Queueing Buffer");
        // and queue it up for a write ...
        _bufferQueue.put(new QueuedBufferItem(_activeBuffer));
      }
    } catch (InterruptedException e) {
    }
    _activeBuffer = null;
  }

  public void spillRawRecord2(byte[] keyData, int keyOffset, int keyLength,
      final byte[] valueData, final int valueOffset, final int valueLength)
  throws IOException {

    if (_indexWriter != null)
      _indexWriter.indexItem(keyData, keyOffset, keyLength, valueData,
          valueOffset, valueLength, writer.getLength());

    _recordCount++;

    writer.appendRaw(keyData, keyOffset, keyLength, new ValueBytes() {

      @Override
      public int getSize() {
        return valueLength;
      }

      @Override
      public void writeCompressedBytes(DataOutputStream outStream)
      throws IllegalArgumentException, IOException {
        throw new IOException("UnSupported Method");
      }

      @Override
      public void writeUncompressedBytes(DataOutputStream outStream)
      throws IOException {
        outStream.write(valueData, valueOffset, valueLength);
      }

    });
  }
}
