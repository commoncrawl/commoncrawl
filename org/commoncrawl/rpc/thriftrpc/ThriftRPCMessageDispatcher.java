/**
 * 
 */
package org.commoncrawl.rpc.thriftrpc;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;

public interface ThriftRPCMessageDispatcher { 
  public void process(ThriftRPCServerChannel serverChannel,ThriftRPCClientChannel clientChannel,TMessage message,TProtocol inputProtocol) throws TException;
}