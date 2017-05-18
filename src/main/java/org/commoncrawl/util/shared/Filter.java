/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.commoncrawl.util.shared;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Method;

/**
 * Base class for BloomFilter implementation
 * 
 * @author rana
 * 
 */
public abstract class Filter {
  int hashCount;

  int getHashCount() {
    return hashCount;
  }

  public int[] getHashBuckets(String key) {
    return Filter.getHashBuckets(key, hashCount, buckets());
  }

  public int[] getHashBuckets(long key) {
    return Filter.getHashBuckets(key, hashCount, buckets());
  }

  abstract int buckets();

  public abstract void add(String key);

  public abstract boolean isPresent(String key);

  // for testing
  abstract int emptyBuckets();

  ICompactSerializer<Filter> getSerializer() {
    Method method = null;
    try {
      method = getClass().getMethod("serializer");
      return (ICompactSerializer<Filter>) method.invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // murmur is faster than a sha-based approach and provides as-good collision
  // resistance. the combinatorial generation approach described in
  // https://www.eecs.harvard.edu/~michaelm/postscripts/tr-02-05.pdf
  // does prove to work in actual tests, and is obviously faster
  // than performing further iterations of murmur.
  static int[] getHashBuckets(String key, int hashCount, int max) {
    byte[] b;
    try {
      b = key.getBytes("UTF-16");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    int[] result = new int[hashCount];
    int hash1 = MurmurHash.hash(b, b.length, 0);
    int hash2 = MurmurHash.hash(b, b.length, hash1);
    for (int i = 0; i < hashCount; i++) {
      result[i] = Math.abs((hash1 + i * hash2) % max);
    }
    return result;
  }

  static int[] getHashBuckets(long key, int hashCount, int max) {
    byte[] b = new byte[8];

    b[0] = (byte) ((key >>> 56) & 0xFF);
    b[1] = (byte) ((key >>> 48) & 0xFF);
    b[2] = (byte) ((key >>> 40) & 0xFF);
    b[3] = (byte) ((key >>> 32) & 0xFF);
    b[4] = (byte) ((key >>> 24) & 0xFF);
    b[5] = (byte) ((key >>> 16) & 0xFF);
    b[6] = (byte) ((key >>> 8) & 0xFF);
    b[7] = (byte) ((key) & 0xFF);

    int[] result = new int[hashCount];
    int hash1 = MurmurHash.hash(b, b.length, 0);
    int hash2 = MurmurHash.hash(b, b.length, hash1);
    for (int i = 0; i < hashCount; i++) {
      result[i] = Math.abs((hash1 + i * hash2) % max);
    }
    return result;
  }

}