package org.commoncrawl.rpc;

/**
 * Message - Encapsulates a Message passed between a sender and receiver. 
 * 
 * A message has a service name (used to group a family of messages)
 * and a message name (used to indentify a specific message).
 * 
 * It also contains a parameterized InputType and a parameterized OutputType. 
 * 
 * Lastly, it has a status representation.  
 * 
 * @author rana
 *
 * @param <InputType>
 * @param <OutputType>
 */
public class MessageData<InputType extends RPCStruct,OutputType extends RPCStruct> {

  public enum Status {
    Success, Error_RPCFailed, Error_ServerError, Error_RequestFailed
  }
  
  

  int                             _requestId;
  InputType                       _inputData;
  OutputType                      _outputData;
  Status                          _status = Status.Success;
  String                          _errorDesc = "";
  
  
  public MessageData(int requestId,InputType inputData,OutputType outputData) {

    _requestId = requestId;
    _inputData = inputData;
    _outputData = outputData;
  }

  public int getRequestId() {
    return _requestId;
  }
  
  public void setRequestId(int requestId) { 
    _requestId = requestId;
  }
  

  public InputType getInput() {
    return _inputData;
  }

  public OutputType getOutput() {
    return _outputData;
  }
  
  public void setOutput(OutputType outputData) { 
    _outputData = outputData;
  }
  
  public Status getStatus() { 
    return _status;
  }
  
  public void setStatus(Status status) { 
    _status = status;
  }
  
  public void setErrorDesc(String desc) { 
    _errorDesc = desc;
  }
  
  public String getErrorDesc() { 
    return _errorDesc;
  }
  
}
