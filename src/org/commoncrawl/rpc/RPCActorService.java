package org.commoncrawl.rpc;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.rpc.RPCFrame.IncomingFrame;

/** 
 * RPCActorService - An object that routes messages from on more RPCServerChannels to 
 * the appropriate user defined handler.
 * 
 * @author rana
 *
 */
public class RPCActorService {
  
  public static final Log LOG = LogFactory.getLog(RPCActorService.class); /** interface specification **/


  /** 
   * RPCActorService A tuple that binds an Actor instance to its specification
   * @author rana
   *
   */
  private static class RPCActorInstance {
    
    RPCActorInstance(ActorInterface.RPCSpecification spec,ActorInterface actorInterface,ThreadPoolExecutor executor) { 
      _specification = spec;
      _instance = actorInterface;
      _threadPool = executor;
    }
    
    public ActorInterface.RPCSpecification _specification;
    public ActorInterface _instance;
    public ThreadPoolExecutor _threadPool;
  }
  
  /**
   * ServerChannelMapItem - A binding between service name and associated 
   * RPCActorInstance
   * 
   * @author rana
   *
   */
  private static class ServerChannelMapItem {
    public Map<String, RPCActorInstance> _serviceMap = new HashMap<String, RPCActorInstance>();
  }

  /**
   * A binding between a RPCServerChannel and a set of ActorInstances
   */
  private final Map<RPCServerChannel, ServerChannelMapItem> _channelMap = new HashMap<RPCServerChannel, ServerChannelMapItem>();

  /**
   * Optional ThreadPool to use to execute incoming message requests
   */
  ThreadPoolExecutor _threadPool;
  
  public RPCActorService(ThreadPoolExecutor optionalThreadPool) { 
    _threadPool = optionalThreadPool;
  }
  
  /**
   * 
   * @param channel The RPCServerChannel (port) that will accept 
   * connections/messages for the specified Actor instance 
   * @param specification The RPCSpecification for the ActorInstance
   * @param actorInstance The Actor instance itself
   * @param optionalThreadPool An Optional ThreadPool to use to execute incoming
   * requests. If none is specified, then the request will execute either 
   * within the EventLoop thread of the bound RPCServerChannel or in a Pooled
   * Thread (if a  ThreadPool is specified at the service level)  
   */
  public final void bindActor(RPCServerChannel channel,
      ActorInterface.RPCSpecification specification, ActorInterface actorInstance,
      ThreadPoolExecutor optionalThreadPool) {

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
      item._serviceMap.put(specification._name, new RPCActorInstance(specification,actorInstance,optionalThreadPool));
    }
  }

  public void start() throws IOException {
    synchronized (_channelMap) {
      for (RPCServerChannel channel : _channelMap.keySet()) {
        channel.open();
      }
    }
  }

  public void stop() {
    synchronized (_channelMap) {
      for (RPCServerChannel channel : _channelMap.keySet()) {
        channel.close();
      }
    }
  }

  public final void dispatchWireRequest(final RPCServerChannel serverChannel,
      final RPCChannel source, final IncomingFrame frame) throws RPCException {

    RPCActorInstance targetInstance = null;
    synchronized (_channelMap) {
      ServerChannelMapItem mapItem = _channelMap.get(serverChannel);

      if (mapItem != null) {
        targetInstance = mapItem._serviceMap.get(frame._service);
      }
    }

    if (targetInstance == null) {
      throw new RPCException("Uknown Service Name:" + frame._service);
    }
    
    final ActorInterface dispatchInstance = targetInstance._instance;
    final DataInputStream payload = new DataInputStream(frame._payload);
    
    final RPCMessageDispatcher dispatcher = targetInstance._specification._dispatcher;
    
    Runnable runnable = new Runnable() {

      @Override
      public void run() {
        try {
          dispatcher.dispatch(dispatchInstance,serverChannel, 
              frame._service,frame._method, payload, frame._requestId, source);
        } catch (RPCException e) {
          IncomingMessageContext<RPCStruct, RPCStruct> dummyContext = new IncomingMessageContext<RPCStruct, RPCStruct>(
              source,frame._requestId, null, null);

          dummyContext.setStatus(OutgoingMessageContext.Status.Error_RPCFailed);
          dummyContext.setErrorDesc(e.toString());
          try {
            source.sendResponse(dummyContext);
          } catch (RPCException e1) {
            LOG.error("RPCException while sending <FAILED> response::"
                + e.toString());
          }
        }
      }
    };
    // ok figure out which context to run the message 
    if (targetInstance._threadPool != null) {
      // target instance has a specific thread pool. use it.
      targetInstance._threadPool.submit(runnable);
    }
    else if (_threadPool != null) {
      // service has a thread pool. use it.
      _threadPool.submit(runnable);
    }
    else { 
      // run it in the current thread's context (eventloop of the 
      // associated ServerChannel
      runnable.run();
    }
  }
}
