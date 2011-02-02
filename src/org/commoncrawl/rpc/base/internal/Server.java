package org.commoncrawl.rpc.base.internal;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.commoncrawl.rpc.base.internal.RPCFrame.IncomingFrame;
import org.commoncrawl.rpc.base.shared.RPCException;

public class Server {

  private static class ServerChannelMapItem {

    public Map<String, Service.Specification> _serviceMap = new HashMap<String, Service.Specification>();

  }

  private final Map<AsyncServerChannel, ServerChannelMapItem> _channelMap = new HashMap<AsyncServerChannel, ServerChannelMapItem>();

  public final void registerService(AsyncServerChannel channel,
      Service.Specification specification) {

    synchronized (_channelMap) {

      ServerChannelMapItem item = _channelMap.get(channel);
      if (item == null) {
        item = new ServerChannelMapItem();
        _channelMap.put(channel, item);
      } else {
        if (item._serviceMap.get(specification._name) != null) {
          throw new RuntimeException(
              "Invalid call to register Service. The specified channel - service specification "
                  + "association already exists!");
        }
      }
      item._serviceMap.put(specification._name, specification);

    }
  }

  public void start() throws IOException {
    synchronized (_channelMap) {
      for (AsyncServerChannel channel : _channelMap.keySet()) {
        channel.open();
      }
    }
  }

  public void stop() {
    synchronized (_channelMap) {
      for (AsyncServerChannel channel : _channelMap.keySet()) {
        channel.close();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void dispatchRequest(AsyncServerChannel serverChannel,
      AsyncClientChannel source, IncomingFrame frame) throws RPCException {

    Service.Specification spec = null;
    synchronized (_channelMap) {
      ServerChannelMapItem mapItem = _channelMap.get(serverChannel);

      if (mapItem != null) {
        spec = mapItem._serviceMap.get(frame._service);
      }
    }

    if (spec == null) {
      throw new RPCException("Uknown Service Name:" + frame._service);
    }
    // dispatch the request via the service's dispatcher ...
    AsyncContext context = spec._dispatcher.dispatch(this, frame,
        serverChannel, source);

    // TODO: if not completed immediately, 
    // add this request to pending request context
    // list ?? 
    /**
    if (!context.isComplete()) {
      // TODO: IS THIS NECESSARY ?
    }
    **/
  }

}
