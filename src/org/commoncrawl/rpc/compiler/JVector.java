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
public class JVector extends JCompType {

  static private int level = 0;

  static private String getId(String id) {
    return id + getLevel();
  }

  static private String getLevel() {
    return Integer.toString(level);
  }

  static private void incrLevel() {
    level++;
  }

  static private void decrLevel() {
    level--;
  }

  class JavaVector extends JavaCompType {

    private JType.JavaType element;

    JavaVector(JType.JavaType t) {
      super("java.util.ArrayList<" + t.getWrapperType() + ">", "Vector",
          "java.util.ArrayList<" + t.getWrapperType() + ">");
      element = t;
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
      element.genReadMethod(cb, getId("e"), getId("e"), true);
      cb.append(fname + ".add(" + getId("e") + ");\n");
      cb.append("}\n");
      decrLevel();
      cb.append("}\n");
    }

    @Override
    void genWriteMethod(CodeBuffer cb, String fname, String tag) {
      cb.append("{\n");
      incrLevel();

      cb.append("int " + getId("len") + " = " + fname + ".size();\n");
      cb.append("encoder.writeInt(output," + getId("len") + ");\n");
      cb.append("for(int " + getId("vidx") + " = 0; " + getId("vidx") + "<"
          + getId("len") + "; " + getId("vidx") + "++) {\n");
      cb.append(element.getType() + " " + getId("e") + " = " + fname + ".get("
          + getId("vidx") + ");\n");
      element.genWriteMethod(cb, getId("e"), getId("e"));
      cb.append("}\n");
      cb.append("}\n");
      decrLevel();
    }

    @Override
    void genClearMethod(CodeBuffer cb, String fname) {
      // TODO: IS THIS THE MOST EFFICIENT WAY TO CLEAR A STRING
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

    private final void genMergeOrClone(boolean isClone, CodeBuffer cb,
        String type, String targetField, String sourceField) {

      cb.append("//Deep Copy Vector\n");

      if (isClone) {
        cb.append(targetField + " = new " + "java.util.ArrayList<"
            + element.getWrapperType() + ">();\n");
      }

      cb.append("int " + getId("len") + " = " + sourceField + ".size();\n");

      cb.append("for(int " + getId("vidx") + " = 0; " + getId("vidx") + "<"
          + getId("len") + "; " + getId("vidx") + "++) {\n");
      // get the next entry ...
      cb.append(element.getType() + " " + getId("src") + " = " + sourceField
          + ".get(" + getId("vidx") + ");\n");
      // declare copied value
      cb.append(element.getType() + " " + getId("tgt") + ";\n");

      // and clone the new element ...
      element.genClone(cb, element.getType(), getId("tgt"), getId("src"));

      // and add the new entry to new vector ...
      cb.append(targetField + ".add(" + getId("tgt") + ");\n");
      cb.append("}\n");
    }

    void genValidFieldCheck(CodeBuffer cb, String fieldName) {
      cb.append("if (" + fieldName + ".size() != 0)");
    }

    /** does this type have an idependent dirty state - ignores validFields bit **/
    boolean hasDirtyState() {
      return true;
    }

    void genDirtyCheck(CodeBuffer cb, String fieldName) {
      cb.append("if (!isDirty){\n");
      cb.append("isDirty=" + fieldName + ".size() !=0;\n");
      cb.append("}\n");
    }

  }

  /** Creates a new instance of JVector */
  public JVector(JType t) {
    setJavaType(new JavaVector(t.getJavaType()));
  }
}
