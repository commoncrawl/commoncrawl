package org.commoncrawl.rpc;

public class OutgoingMessageContext<InputType extends RPCStruct, OutputType extends RPCStruct> 
 extends MessageData<InputType,OutputType>{

  public static interface Callback<InputType extends RPCStruct, OutputType extends RPCStruct> {
    void requestComplete(OutgoingMessageContext<InputType,OutputType> request);
  }

  Callback<InputType, OutputType> _callback = null;
  String _serviceName;
  String _methodName;
  
  
  public OutgoingMessageContext(String serviceName, String method, InputType input,
      OutputType output, Callback<InputType, OutputType> callback) {
    
    super(0,input,output);
    _callback = callback;
    _serviceName = serviceName;
    _methodName = method;
  }

  @SuppressWarnings("unchecked")
  public Callback getCallback() {
    return _callback;
  }
  
  public String getServiceName() { 
    return _serviceName;
  }
  
  public String getMethodName() { 
    return _methodName;
  }
}
