package org.commoncrawl.rpc.compiler;

public class JMethod {

  public String  _methodName;
  public JRecord _inputType;
  public JRecord _outputType;

  public JMethod(String methodName, JRecord input, JRecord output) {
    _methodName = methodName;
    _inputType = input;
    _outputType = output;
  }

  public String getName() {
    return _methodName;
  }

  public JRecord getInputType() {
    return _inputType;
  }

  public JRecord getOutputType() {
    return _outputType;
  }
}
