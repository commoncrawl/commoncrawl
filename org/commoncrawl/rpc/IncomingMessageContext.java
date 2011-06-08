package org.commoncrawl.rpc;


/**
 * MessageData - Contains Input/Output Parameters for a message, tracks the 
 * channel the message came in on and provides for a way for receiver to 
 * complete a request (and trigger a response callback).
 * 
 * @author rana
 *
 * @param <InputType>
 * @param <OutputType>
 */
public class IncomingMessageContext<InputType extends RPCStruct, OutputType extends RPCStruct> 
   extends MessageData<InputType,OutputType> {


  Channel                         _channel;
  boolean                         _isComplete = false;

  
  public IncomingMessageContext(Channel channel,int requestId, InputType input,OutputType output) {
    super(requestId,input,output);
    _channel    = channel;
  }

  boolean isComplete() {
    return _isComplete;
  }

  public Channel getChannel() {
    return _channel;
  }

  public void completeRequest() throws RPCException {
    _channel.sendResponse(this);
  }
  
  public int getRequestId() {
    return _requestId;
  }
  
  public void setRequestId(int requestId) { 
    _requestId = requestId;
  }
}
