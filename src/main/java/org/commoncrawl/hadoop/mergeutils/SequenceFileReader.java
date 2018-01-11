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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.commoncrawl.util.shared.CCStringUtils;

/**
 * An input source that reads from a SequenceFile
 * 
 * @author rana
 * 
 * @param <KeyType>
 * @param <ValueType>
 */
public class SequenceFileReader<KeyType extends WritableComparable, ValueType extends Writable> {

  public static final Log LOG = LogFactory.getLog(SequenceFileReader.class);

  FileSystem _sourceFileSystem;
  Configuration _config;
  SpillWriter<KeyType, ValueType> _writer = null;
  Vector<Path> _inputSegments = null;
  Constructor<KeyType> _keyConstructor = null;
  Constructor<ValueType> _valConstructor = null;
  long _recordCount = 0;
  private static final Class[] emptyArray = new Class[] {};

  public SequenceFileReader(FileSystem fileSystem, Configuration conf,
      Vector<Path> inputSegments, SpillWriter<KeyType, ValueType> spillWriter,
      Class<KeyType> keyClass, Class<ValueType> valueClass) throws IOException {

    _sourceFileSystem = fileSystem;
    _config = conf;
    _inputSegments = inputSegments;
    _writer = spillWriter;

    try {
      this._keyConstructor = keyClass.getDeclaredConstructor(emptyArray);
      this._keyConstructor.setAccessible(true);
      this._valConstructor = valueClass.getDeclaredConstructor(emptyArray);
      this._valConstructor.setAccessible(true);
    } catch (SecurityException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }

  }

  public void close() throws IOException {
  }

  @SuppressWarnings("unchecked")
  public void readAndSpill() throws IOException {
    long cumilativeReadTimeStart = System.currentTimeMillis();

    for (Path sequenceFilePath : _inputSegments) {

      long individualReadTimeStart = System.currentTimeMillis();

      // LOG.info("Reading Contents for File:" + sequenceFilePath);
      // allocate a reader for the current path
      SequenceFile.Reader reader = new SequenceFile.Reader(_sourceFileSystem,
          sequenceFilePath, _config);

      try {
        boolean eos = false;

        while (!eos) {

          KeyType key = null;
          ValueType value = null;

          try {
            key = _keyConstructor.newInstance();
            value = _valConstructor.newInstance();
          } catch (Exception e) {
            LOG.error("Failed to create key or value type with Exception:"
                + CCStringUtils.stringifyException(e));
            throw new RuntimeException(e);
          }

          eos = !reader.next(key, value);

          if (!eos) {
            _recordCount++;
            _writer.spillRecord(key, value);
          }
        }
        while (!eos)
          ;

        // LOG.info("Read and Spill of File:" + sequenceFilePath +" took:" +
        // (System.currentTimeMillis() - individualReadTimeStart));
      } finally {

        if (reader != null) {
          reader.close();
        }
      }
    }
    // LOG.info("Cumilative Read and Spill took:" + (System.currentTimeMillis()
    // - cumilativeReadTimeStart) + " Spilled RecordCount:" + _recordCount);
  }
}
