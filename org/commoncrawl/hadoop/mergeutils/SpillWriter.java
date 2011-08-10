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
 * Your basic OutputSink, take typed Key,Value tuples and writes them to some
 * abstract storage destination
 * 
 * @author rana
 * 
 * @param <KeyType>
 *          a WritableComparable derived class type
 * @param <ValueType>
 *          a Writable derived class type
 */
public interface SpillWriter<KeyType extends WritableComparable, ValueType extends Writable> {

  void spillRecord(KeyType key, ValueType value) throws IOException;

  void close() throws IOException;

}
