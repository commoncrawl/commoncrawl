package org.commoncrawl.rpc.compiler;

public class JEnumValue {

  private String name;
  private int    value;

  public JEnumValue(String name, int value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public int getValue() {
    return value;
  }
}
