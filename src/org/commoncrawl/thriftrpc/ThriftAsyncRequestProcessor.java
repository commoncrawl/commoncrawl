/**
 * 
 */
package org.commoncrawl.thriftrpc;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;

public interface ThriftAsyncRequestProcessor { 
  public void process(ThriftAsyncServerChannel serverChannel,ThriftAsyncRemoteClientChannel clientChannel,TMessage message,TProtocol inputProtocol) throws TException;
}