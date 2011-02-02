package org.commoncrawl.rpc.base.internal;

import org.commoncrawl.rpc.base.shared.RPCException;

public interface RequestDispatcher {

  @SuppressWarnings("unchecked")
  AsyncContext dispatch(Server server, RPCFrame.IncomingFrame frame,
      AsyncServerChannel serverChannel, AsyncClientChannel clientChannel)
      throws RPCException;
}
