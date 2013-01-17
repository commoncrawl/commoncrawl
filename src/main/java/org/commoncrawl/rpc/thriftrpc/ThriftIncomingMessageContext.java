package org.commoncrawl.rpc.thriftrpc;

import java.io.IOException;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.commoncrawl.io.internal.NIOBufferList;
import org.commoncrawl.io.internal.NIOBufferListOutputStream;
import org.commoncrawl.util.shared.CCStringUtils;

/**
 * Class that encapsulates an async dispatch context
 * 
 * @author rana
 *
 */
@SuppressWarnings("unchecked")
public class ThriftIncomingMessageContext<InputArgs extends TBase,OutputArgs extends TBase> {
  
  TProtocol iprot;
  TProtocol oprot;
  NIOBufferList responseBuffer = new NIOBufferList();
  TMessage  msg = null;
  InputArgs inputArgs;
  OutputArgs outputArgs;
  ThriftRPCServerChannel serverChannel;
  ThriftRPCClientChannel clientChannel;
  
  public ThriftIncomingMessageContext(ThriftRPCServerChannel serverChannel,ThriftRPCClientChannel channel,TMessage message,TProtocol inputProtocol,InputArgs inputArgs,OutputArgs outputArgs) throws TException {
    this.serverChannel = serverChannel;
    this.clientChannel = channel;
    // initialize input protocol 
    this.iprot = inputProtocol;
    this.msg = message;
    this.inputArgs = inputArgs;
    this.outputArgs = outputArgs;
    
    // and setup output protocol to write the local buffer 
    this.oprot = channel.getOutputProtocolFactory().getProtocol(
        new TFramedTransport(new TIOStreamTransport(
        new NIOBufferListOutputStream(responseBuffer)    
        )));
     
     // start writing out outgoing message ...
     oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage(this.msg.name, org.apache.thrift.protocol.TMessageType.REPLY, this.msg.seqid));
  }
  
  /**
   * 
   * @return InputArgs data structure 
   */
  public InputArgs getInput() { 
    return inputArgs;
  }
  
  /**
   * 
   * @return OutputArgs data structure 
   */
  public OutputArgs getOutput() { 
    return outputArgs;
  }
  
  /** 
   * 
   * @return the server channel associated with this request 
   */
  public ThriftRPCServerChannel getServerChannel() { 
    return serverChannel;
  }

  /**
   * 
   * @return the remote client channel associated with the request..
   */
  public ThriftRPCClientChannel getClientChannel() { 
    return clientChannel;
  }
  
  /**
   * asynchronously complete the request... 
   * you should have already populated OutputArgs ... 
   * 
   * @throws TException
   */
  public void completeRequest() throws TException {
    // write result ... 
    outputArgs.write(oprot);
    // end outgoing message ... 
    oprot.writeMessageEnd();
    // flush 
    oprot.getTransport().flush();
    // send response across wire 
    try {
      clientChannel.responseReady(this.responseBuffer);
    } catch (IOException e) {
      ThriftRPCServerChannel.LOG.error(CCStringUtils.stringifyException(e));
      // close channel... we are hosed 
      clientChannel.close();
      // bubble up
      throw new TException(e);
    }
  }

  /**
   * This method will be called when there is an unexpected clientside
   * exception. This does not include application-defined exceptions that
   * appear in the IDL, but rather things like IOExceptions.
   * @param exception
   */
  public void failRequest(Exception e)throws TException {
    org.apache.thrift.TApplicationException x = new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.PROTOCOL_ERROR, e.getMessage());
    oprot.writeMessageBegin(new org.apache.thrift.protocol.TMessage(msg.name, org.apache.thrift.protocol.TMessageType.EXCEPTION, msg.seqid));
    x.write(oprot);
    oprot.writeMessageEnd();
    oprot.getTransport().flush();
    try {
      clientChannel.responseReady(this.responseBuffer);
    } catch (IOException e1) {
      ThriftRPCServerChannel.LOG.error(CCStringUtils.stringifyException(e1));
      // close channel... we are hosed
      clientChannel.close();
      // bubble up
      throw new TException(e1);
    }
  }    
}