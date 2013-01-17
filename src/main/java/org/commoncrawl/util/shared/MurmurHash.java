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

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 * 
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */
public class MurmurHash {

  static final int m = 0x5bd1e995;
  static final int r = 24;

  public static final int hashInt(int k, int seed) {
    int k1 = k * m;
    int k2 = k1 ^ (k1 >>> r);
    int k3 = k2 * m;
    int h1 = seed * m;
    return h1 ^ k3;
  }

  public static final int hashLong(long k, int seed) {
    int h = hashInt((int) ((k >> 32) & Integer.MAX_VALUE), seed);
    int h2 = hashInt(((int) k), h);
    return h2;
  }

  public static final int hashFloat(float f, int seed) {
    return hashInt(Float.floatToIntBits(f), seed);
  }

  public static final int hashDouble(double d, int seed) {
    return hashLong(Double.doubleToLongBits(d), seed);
  }

  public static final int hashBoolean(boolean b, int seed) {
    return hashInt((b) ? 1 : 0, seed);
  }

  public static final int hashString(String s, int seed) {
    byte[] bytes = s.getBytes();
    return hash(bytes, bytes.length, seed);
  }

  public static final int hash(byte[] data, int length, int seed) {
    return hash(data, 0, length, seed);
  }

  public static final int hash(byte[] data, int offset, int length, int seed) {

    int h = seed ^ length;

    int len_4 = length >> 2;

    for (int i = 0; i < len_4; i++) {
      int i_4 = i << 2;
      int k = data[offset + i_4 + 3];
      k = k << 8;
      k = k | (data[offset + i_4 + 2] & 0xff);
      k = k << 8;
      k = k | (data[offset + i_4 + 1] & 0xff);
      k = k << 8;
      k = k | (data[offset + i_4 + 0] & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }

    // avoid calculating modulo
    int len_m = len_4 << 2;
    int left = length - len_m;

    if (left != 0) {
      if (left >= 3) {
        h ^= (int) data[offset + length - 3] << 16;
      }
      if (left >= 2) {
        h ^= (int) data[offset + length - 2] << 8;
      }
      if (left >= 1) {
        h ^= (int) data[offset + length - 1];
      }

      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    return h;
  }
}
