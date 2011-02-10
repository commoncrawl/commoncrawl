package org.commoncrawl.rpc;

import java.io.DataInputStream;

/**
 * An abstraction used to an object that knows how to dispatch an incoming
 * RPC Message.
 * 
 * @author rana
 *
 */
public interface RPCMessageDispatcher {

  void dispatch(ActorInterface instance,RPCServerChannel serverChannel, String serviceId,String messageId,DataInputStream messagePayload,
      int requestId,Channel clientCannel)
      throws RPCException;
}
