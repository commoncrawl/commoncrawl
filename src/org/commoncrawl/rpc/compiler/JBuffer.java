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
 * Code generator for "buffer" type.
 */
public class JBuffer extends JCompType {

  class JavaBuffer extends JavaCompType {

    JavaBuffer() {
      super("FlexBuffer", "FlexBuffer",
          "org.commoncrawl.util.shared.FlexBuffer");
    }

    @Override
    void genEquals(CodeBuffer cb, String fname, String peer) {
      cb.append("ret = " + fname + ".equals(" + peer + ");\n");
    }

    @Override
    void genHashCode(CodeBuffer cb, String fname) {
      cb.append("result = MurmurHash.hash(" + fname + ".get()," + fname
          + ".getOffset()," + fname + ".getCount(),result);\n");
    }

    @Override
    void genClearMethod(CodeBuffer cb, String fname) {
      cb.append(fname + ".reset();\n");
    }

    @Override
    void genGetSet(CodeBuffer cb,String className, String fname, boolean trackDirtyFields) {
      cb.append("public ImmutableBuffer get" + toCamelCase(fname) + "() {\n");
      cb.append("return new ImmutableBuffer(" + fname + ");\n");
      cb.append("}\n");
      cb.append("public "+className+" set" + toCamelCase(fname) + "( " + getType() + " "
          + fname + ") {\n");
      if (trackDirtyFields) {
        cb.append("__validFields.set(Field_" + fname.toUpperCase() + ");\n");
      }
      cb.append("this." + fname + "=" + fname + ";\n");
      cb.append("return this;\n");
      cb.append("}\n");
      cb.append("public " +className +" set" + toCamelCase(fname) + "( Buffer " + fname
          + ",boolean shared) {\n");
      if (trackDirtyFields) {
        cb.append("__validFields.set(Field_" + fname.toUpperCase() + ");\n");
      }
      cb.append("this." + fname + "= new FlexBuffer(" + fname + ".get(),0,"
          + fname + ".getCount(),shared);\n");
      cb.append("return this;\n");
      cb.append("}\n");

    }

    @Override
    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(getType() + " " + fname + " = new FlexBuffer();\n");
      }
      cb.append("decoder.read" + getMethodSuffix() + "(input," + fname+ ");\n");    
    }
  }

  /** Creates a new instance of JBuffer */
  public JBuffer() {
    setJavaType(new JavaBuffer());
  }

  String getSignature() {
    return "B";
  }

  boolean isComparable() {
    return true;
  }

}
