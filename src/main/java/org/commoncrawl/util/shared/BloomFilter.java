/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.util.shared;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;

/**
 * 
 * Basic 32 bit (memory model) implementation of Bloom Filters
 * 
 * @author rana
 * 
 */
public class BloomFilter extends Filter {
  static ICompactSerializer<BloomFilter> serializer_ = new BloomFilterSerializer();

  public static ICompactSerializer<BloomFilter> serializer() {
    return serializer_;
  }

  private BitSet filter_;

  public BloomFilter(int numElements, int bucketsPerElement) {
    this(BloomCalculations.computeBestK(bucketsPerElement), new BitSet(
        numElements * bucketsPerElement + 20));
  }

  public BloomFilter(int numElements, double maxFalsePosProbability) {
    BloomCalculations.BloomSpecification spec = BloomCalculations
        .computeBucketsAndK(maxFalsePosProbability);
    filter_ = new BitSet(numElements * spec.bucketsPerElement + 20);
    hashCount = spec.K;
  }

  /*
   * This version is only used by the deserializer.
   */
  BloomFilter(int hashes, BitSet filter) {
    hashCount = hashes;
    filter_ = filter;
  }

  public void clear() {
    filter_.clear();
  }

  int buckets() {
    return filter_.size();
  }

  BitSet filter() {
    return filter_;
  }

  public boolean isPresent(String key) {
    for (int bucketIndex : getHashBuckets(key)) {
      if (!filter_.get(bucketIndex)) {
        return false;
      }
    }
    return true;
  }

  public boolean isPresent(long key) {
    for (int bucketIndex : getHashBuckets(key)) {
      if (!filter_.get(bucketIndex)) {
        return false;
      }
    }
    return true;
  }

  /*
   * param@ key -- value whose hash is used to fill the filter_. This is a
   * general purpose API.
   */
  public void add(String key) {
    for (int bucketIndex : getHashBuckets(key)) {
      filter_.set(bucketIndex);
    }
  }

  public void add(long key) {
    for (int bucketIndex : getHashBuckets(key)) {
      filter_.set(bucketIndex);
    }
  }

  public String toString() {
    return filter_.toString();
  }

  @SuppressWarnings("unchecked")
  ICompactSerializer tserializer() {
    return serializer_;
  }

  int emptyBuckets() {
    int n = 0;
    for (int i = 0; i < buckets(); i++) {
      if (!filter_.get(i)) {
        n++;
      }
    }
    return n;
  }

  static class BloomFilterSerializer implements ICompactSerializer<BloomFilter> {
    public void serialize(BloomFilter bf, DataOutputStream dos)
        throws IOException {
      dos.writeInt(bf.getHashCount());
      BitSetSerializer.serialize(bf.filter(), dos);
    }

    public BloomFilter deserialize(DataInputStream dis) throws IOException {
      int hashes = dis.readInt();
      BitSet bs = BitSetSerializer.deserialize(dis);
      return new BloomFilter(hashes, bs);
    }
  }

  static class BitSetSerializer {
    public static void serialize(BitSet bs, DataOutputStream dos)
        throws IOException {
      ObjectOutputStream oos = new ObjectOutputStream(dos);
      oos.writeObject(bs);
      oos.flush();
    }

    public static BitSet deserialize(DataInputStream dis) throws IOException {
      ObjectInputStream ois = new ObjectInputStream(dis);
      try {
        return (BitSet) ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
