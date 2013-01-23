package org.commoncrawl.util.shared;

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
 * 
 */


import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * very basic (character based) shingle support
 * 
 * @author rana
 * 
 */
public class Shingle {

  public static final int CHAR_GRAM_LENGTH = 3;

  public static Set<String> shingles(String line) {

    HashSet<String> shingles = new HashSet<String>();

    for (int i = 0; i < line.length() - CHAR_GRAM_LENGTH + 1; i++) {
      // extract an ngram
      String shingle = line.substring(i, i + CHAR_GRAM_LENGTH);
      // get it's index from the dictionary
      shingles.add(shingle);
    }
    return shingles;
  }

  public static float jaccard_similarity_coeff(Set<String> shinglesA,
      Set<String> shinglesB) {
    float neumerator = Sets.intersection(shinglesA, shinglesB).size();
    float denominator = Sets.union(shinglesA, shinglesB).size();
    return neumerator / denominator;
  }
}
