package org.commoncrawl.rpc.base.internal;

import org.commoncrawl.rpc.base.shared.RPCException;

public interface Dispatcher {
		
	@SuppressWarnings("unchecked")
  AsyncContext dispatch(Server server,Frame.IncomingFrame frame,AsyncServerChannel serverChannel,AsyncClientChannel clientChannel) throws RPCException;
}
