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
 * Abstract base class for all the "compound" types such as ustring, buffer,
 * vector, map, and record.
 */
abstract class JCompType extends JType {

  abstract class JavaCompType extends JavaType {

    JavaCompType(String type, String suffix, String wrapper) {
      super(type, suffix, wrapper);
    }

    @Override
    void genDecl(CodeBuffer cb, String fname) {
      cb.append("private " + getType() + " " + fname + "=  new " + getType()
          + "();\n");
    }

    @Override
    void genEquals(CodeBuffer cb, String fname, String peer) {
      cb.append("ret = " + fname + ".equals(" + peer + ");\n");
    }

    @Override
    void genHashCode(CodeBuffer cb, String fname) {
      cb.append("result = MurmurHash.hashInt(" + fname
          + ".hashCode(),result);\n");
    }

    @Override
    void genClone(CodeBuffer cb, String type, String targetField,
        String sourceField) {
      cb
          .append(targetField + " = (" + type + ")" + sourceField
              + ".clone();\n");
    }
  }
}
