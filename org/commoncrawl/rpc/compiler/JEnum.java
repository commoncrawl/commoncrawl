package org.commoncrawl.rpc.compiler;

import java.util.ArrayList;

/**
 */
public class JEnum extends JType {

  private String                name;
  private ArrayList<JEnumValue> values;

  /** Creates a new instance of JEnum */

  public JEnum(String name, ArrayList<JEnumValue> values) {
    this.name = name;
    this.values = values;
  }

  public String getName() {
    return name;
  }

  public ArrayList<JEnumValue> getValues() {
    return values;
  }

  String getSignature() {
    return "e";
  }
}
