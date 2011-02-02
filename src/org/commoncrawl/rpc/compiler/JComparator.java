package org.commoncrawl.rpc.compiler;

import java.util.ArrayList;

public class JComparator extends JType {

  private String            name;
  private ArrayList<String> fieldNames;

  /** Creates a new instance of JEnum */

  public JComparator(String name, ArrayList<String> fieldNames) {
    this.name = name;
    this.fieldNames = fieldNames;
  }

  public String getName() {
    return name;
  }

  public ArrayList<String> getFieldNames() {
    return fieldNames;
  }

  String getSignature() {
    return "C";
  }
}
