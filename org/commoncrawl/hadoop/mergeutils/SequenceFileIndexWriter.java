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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("unchecked")
public interface SequenceFileIndexWriter<KeyType extends WritableComparable, ValueType extends Writable> {

  /**
   * index a given item
   * 
   * @param keyData
   *          key bytes
   * @param keyOffset
   *          key offset
   * @param keyLength
   *          key length
   * @param valueData
   *          value bytes
   * @param valueOffset
   *          value offset
   * @param valueLength
   *          value length
   * @param writerPosition
   *          the sequence writer file position for the current item
   */
  void indexItem(byte[] keyData, int keyOffset, int keyLength,
      byte[] valueData, int valueOffset, int valueLength, long writerPosition)
  throws IOException;

  /**
   * flush and close the index file
   * 
   * @throws IOException
   */
  void close() throws IOException;

}
