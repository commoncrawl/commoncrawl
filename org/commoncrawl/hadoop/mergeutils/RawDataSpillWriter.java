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

/**
 * Extends SpillWriter with the ability to handle RAW (as well as Typed) records
 * 
 * @author rana
 * 
 * @param <KeyType>
 * @param <ValueType>
 */
public interface RawDataSpillWriter<KeyType extends WritableComparable, ValueType extends Writable>
extends SpillWriter<KeyType, ValueType> {

  /**
   * spill a key/value pair in raw format
   * 
   * @param keyData
   * @param keyOffset
   * @param keyLength
   * @param valueData
   * @param valueOffset
   * @param valueLength
   * @throws IOException
   */
  void spillRawRecord(byte[] keyData, int keyOffset, int keyLength,
      byte[] valueData, int valueOffset, int valueLength) throws IOException;

}
