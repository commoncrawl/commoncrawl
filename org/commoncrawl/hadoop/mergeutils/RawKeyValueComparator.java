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
 * Extends KeyValuePairComparator by adding support for comparing RAW records
 * (as well as Typed records).
 * 
 * @author rana
 * 
 * @param <KeyType>
 * @param <ValueType>
 */
public interface RawKeyValueComparator<KeyType extends WritableComparable, ValueType extends Writable>
extends KeyValuePairComparator<KeyType, ValueType> {

  /**
   * compare two key value pairs in raw buffer format
   * 
   * @param key1Data
   * @param key1Offset
   * @param key1Length
   * @param key2Data
   * @param key2Offset
   * @param key2Length
   * @param value1Data
   * @param value1Offset
   * @param value1Length
   * @param value2Data
   * @param value2Offset
   * @param value2Length
   * @return
   * @throws IOException
   */
  int compareRaw(byte[] key1Data, int key1Offset, int key1Length,
      byte[] key2Data, int key2Offset, int key2Length, byte[] value1Data,
      int value1Offset, int value1Length, byte[] value2Data, int value2Offset,
      int value2Length) throws IOException;;

}
