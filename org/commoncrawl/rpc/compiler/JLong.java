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

package org.commoncrawl.rpc.compiler;

/**
 * Code generator for "long" type
 */
public class JLong extends JType {

  class JavaLong extends JavaType {

    boolean _variableBitRateEncoding = false;

    JavaLong(boolean variableEncoding) {
      super("long", "Long", "Long");
      _variableBitRateEncoding = variableEncoding;
    }

    boolean isVariableBitRateEncoded() {
      return _variableBitRateEncoding;
    }

    @Override
    void genHashCode(CodeBuffer cb, String fname) {
      cb.append("result = MurmurHash.hashLong(" + fname + ",result);\n");
    }

    @Override
    void genClearMethod(CodeBuffer cb, String fname) {
      cb.append(fname + "=0;\n");
    }
  }

  /** Creates a new instance of JLong */
  public JLong(boolean isVariableEncoded) {
    setJavaType(new JavaLong(isVariableEncoded));
  }

  String getSignature() {
    return "l";
  }

  boolean isComparable() {
    return true;
  }

}
