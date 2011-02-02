package org.commoncrawl.rpc.base.internal;

import org.commoncrawl.rpc.base.shared.RPCException;
import org.commoncrawl.rpc.base.shared.RPCStruct;

public class AsyncContext<InputType extends RPCStruct, OutputType extends RPCStruct> {

  AsyncClientChannel  _clientChannel;
  AsyncServerChannel  _serverChannel;
  int                 _requestId;
  InputType           _input;
  OutputType          _output;
  AsyncRequest.Status _status     = AsyncRequest.Status.Success;
  String              _errorDescription;
  boolean             _isComplete = false;

  public AsyncContext(AsyncServerChannel serverChannel,
      AsyncClientChannel clientChannel, int requestId, InputType input,
      OutputType output) {

    _clientChannel = clientChannel;
    _serverChannel = serverChannel;
    _requestId = requestId;
    _input = input;
    _output = output;

  }

  boolean isComplete() {
    return _isComplete;
  }

  public AsyncClientChannel getClientChannel() {
    return _clientChannel;
  }

  public AsyncServerChannel getServerChannel() {
    return _serverChannel;
  }

  public int getRequestId() {
    return _requestId;
  }

  public InputType getInput() {
    return _input;
  }

  public OutputType getOutput() {
    return _output;
  }

  public void setOutput(OutputType output) {
    _output = output;
  }

  public AsyncRequest.Status getStatus() {
    return _status;
  }

  public void setStatus(AsyncRequest.Status status) {
    _status = status;
  }

  public String getErrorDesc() {
    return _errorDescription;
  }

  public void setErrorDesc(String errorDesc) {
    _errorDescription = errorDesc;
  }

  public void completeRequest() throws RPCException {
    _serverChannel.sendResponse(this);
  }
}
