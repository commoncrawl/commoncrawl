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

import java.net.URL;

import org.commoncrawl.util.shared.FPGenerator;

/**
 * Helper utility to convert a url to a fingerprint. 
 * 
 * People should use FPGenerator instead.
 * 
 * @author rana
 *
 */
public class URLFingerprint {

  public static long generate64BitURLFPrint(URL theURL) {
    return FPGenerator.std64.fp(theURL.toString());
  }

  public static long generate64BitURLFPrint(String theURL) {
    return FPGenerator.std64.fp(theURL);
  }

  public static int generate32BitHostFP(String hostname) {
    return hostname.hashCode();
  }
}
