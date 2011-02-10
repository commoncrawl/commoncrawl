package org.commoncrawl.rpc;



/**
 * A communication channel between a message sender and a message recepient. 
 * 
 * @author rana
 *
 */
public interface Channel {
  
  /**
   * send a request via the channel  
   * 
   * @param the outgoing message to send to the receiver  
   * @throws RPCException
   */
  public void sendRequest(OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct> message) throws RPCException;

  
  /**
   * send a response for a previously receivied message via the channel 
   * 
   * @param the incoming message that is being responded to 
   * @throws RPCException
   */
  public void sendResponse(IncomingMessageContext<? extends RPCStruct,? extends RPCStruct> message) throws RPCException;
  

}
