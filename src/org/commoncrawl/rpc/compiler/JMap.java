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
 */
public class JMap extends JCompType {

  static private int level = 0;

  static private String getLevel() {
    return Integer.toString(level);
  }

  static private void incrLevel() {
    level++;
  }

  static private void decrLevel() {
    level--;
  }

  static private String getId(String id) {
    return id + getLevel();
  }

  class JavaMap extends JavaCompType {

    JType.JavaType key;
    JType.JavaType value;

    JavaMap(JType.JavaType key, JType.JavaType value) {
      super("java.util.TreeMap<" + key.getWrapperType() + ","
          + value.getWrapperType() + ">", "Map", "java.util.TreeMap<"
          + key.getWrapperType() + "," + value.getWrapperType() + ">");
      this.key = key;
      this.value = value;
    }

    @Override
    void genReadMethod(CodeBuffer cb, String fname, String tag, boolean decl) {
      if (decl) {
        cb.append(getType() + " " + fname + ";\n");
      }
      cb.append("{\n");
      incrLevel();

      cb.append("int " + getId("itemCount") + " = decoder.readInt(input);\n");
      cb.append(fname + "=new " + getType() + "();\n");
      cb.append("for (int " + getId("i") + "=0;" + getId("i") + "<"
          + getId("itemCount") + ";++" + getId("i") + ") {\n");
      key.genReadMethod(cb, getId("k"), getId("k"), true);
      value.genReadMethod(cb, getId("v"), getId("v"), true);
      cb.append(fname + ".put(" + getId("k") + "," + getId("v") + ");\n");
      cb.append("}\n");
      decrLevel();
      cb.append("}\n");
    }

    @Override
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      String setType = "java.util.Set<java.util.Map.Entry<"
          + key.getWrapperType() + "," + value.getWrapperType() + ">> ";
      String entryType = "java.util.Map.Entry<" + key.getWrapperType() + ","
          + value.getWrapperType() + "> ";
      String iterType = "java.util.Iterator<java.util.Map.Entry<"
          + key.getWrapperType() + "," + value.getWrapperType() + ">> ";

      cb.append("{\n");
      incrLevel();
      cb.append(setType + getId("es") + " = " + fname + ".entrySet();\n");

      // write out size of set ...
      cb.append("// write out set size\n");
      cb.append("encoder.writeInt(output," + getId("es") + ".size());\n");
      // write out entries ...
      cb.append("for(" + iterType + getId("midx") + " = " + getId("es")
          + ".iterator(); " + getId("midx") + ".hasNext();) {\n");
      cb.append(entryType + getId("me") + " = " + getId("midx") + ".next();\n");
      cb.append(key.getType() + " " + getId("k") + " = " + getId("me")
          + ".getKey();\n");
      cb.append(value.getType() + " " + getId("v") + " = " + getId("me")
          + ".getValue();\n");
      key.genWriteMethod(cb, getId("k"), getId("k"));
      value.genWriteMethod(cb, getId("v"), getId("v"));
      cb.append("}\n");
      cb.append("}\n");
      decrLevel();
    }

    @Override
    void genClearMethod(CodeBuffer cb, String fname) {
      cb.append(fname + ".clear();\n");
    }

    @Override
    void genClone(CodeBuffer cb, String type, String targetField,
        String sourceField) {
      genMergeOrClone(true, cb, type, targetField, sourceField);
    }

    @Override
    void genMerge(CodeBuffer cb, String type, String targetField,
        String sourceField) {
      genMergeOrClone(false, cb, type, targetField, sourceField);
    }

    private void genMergeOrClone(boolean isClone, CodeBuffer cb, String type,
        String targetField, String sourceField) {

      cb.append("//Deep Copy Map\n");
      if (isClone) {
        cb.append(targetField + " = new " + "java.util.TreeMap<"
            + key.getWrapperType() + "," + value.getWrapperType() + ">();\n");
      }

      String setType = "java.util.Set<java.util.Map.Entry<"
          + key.getWrapperType() + "," + value.getWrapperType() + ">> ";

      String entryType = "java.util.Map.Entry<" + key.getWrapperType() + ","
          + value.getWrapperType() + "> ";

      String iterType = "java.util.Iterator<java.util.Map.Entry<"
          + key.getWrapperType() + "," + value.getWrapperType() + ">> ";

      cb.append(setType + getId("es") + " = " + sourceField + ".entrySet();\n");

      cb.append("for(" + iterType + getId("midx") + " = " + getId("es")
          + ".iterator(); " + getId("midx") + ".hasNext();) {\n");
      // get the next entry ...
      cb.append(entryType + getId("me") + " = " + getId("midx") + ".next();\n");
      // declare key and value locals
      cb.append(key.getType() + " " + getId("k") + ";\n");
      cb.append(value.getType() + " " + getId("v") + ";\n");

      // and clone a new key ...
      key.genClone(cb, key.getType(), getId("k"), getId("me") + ".getKey()");
      value.genClone(cb, value.getType(), getId("v"), getId("me")
          + ".getValue()");

      // and set it in the new map ...
      cb.append(targetField + ".put(" + getId("k") + "," + getId("v") + ");\n");
      cb.append("}\n");
    }

    void genValidFieldCheck(CodeBuffer cb, String fieldName) {
      cb.append("if (" + fieldName + ".size() != 0)");
    }

    /** does this type have an idependent dirty state - ignores validFields bit **/
    boolean hasDirtyState() {
      return false;
    }

    void genDirtyCheck(CodeBuffer cb, String fieldName) {
      cb.append("if (!isDirty){\n");
      cb.append("isDirty=" + fieldName + ".size() !=0;\n");
      cb.append("}\n");
    }

  }

  /** Creates a new instance of JMap */
  public JMap(JType t1, JType t2) {
    setJavaType(new JavaMap(t1.getJavaType(), t2.getJavaType()));
  }
}
