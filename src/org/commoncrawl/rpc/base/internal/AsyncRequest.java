package org.commoncrawl.rpc.base.internal;

import org.commoncrawl.rpc.base.shared.RPCStruct;

public class AsyncRequest<InputType extends RPCStruct, OutputType extends RPCStruct> {

  public enum Status {
    Success, Error_RPCFailed, Error_ServerError, Error_RequestFailed
  }

  public AsyncRequest(String serviceName, String method, InputType input,
      OutputType output, Callback<InputType, OutputType> callback) {
    _service = serviceName;
    _method = method;
    _input = input;
    _output = output;
    _callback = callback;
  }

  public static interface Callback<InputType extends RPCStruct, OutputType extends RPCStruct> {
    void requestComplete(AsyncRequest<InputType, OutputType> request);
  }

  String                          _service;
  String                          _method;
  int                             _id;
  InputType                       _input;
  OutputType                      _output;
  Status                          _status   = Status.Error_RPCFailed;
  String                          _errorDesc;
  Callback<InputType, OutputType> _callback = null;

  String getService() {
    return _service;
  }

  String getMethod() {
    return _method;
  }

  public void setRequestId(int requestId) {
    _id = requestId;
  }

  public int getRequestId() {
    return _id;
  }

  public InputType getInput() {
    return _input;
  }

  public OutputType getOutput() {
    return _output;
  }

  public void setStatus(Status status) {
    _status = status;
  }

  public Status getStatus() {
    return _status;
  }

  void setErrorDesc(String errorDesc) {
    _errorDesc = errorDesc;
  }

  String getErrorDesc() {
    return _errorDesc;
  }

  public Callback<InputType, OutputType> getCallback() {
    return _callback;
  }
}
