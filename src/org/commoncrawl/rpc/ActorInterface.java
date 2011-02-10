package org.commoncrawl.rpc;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.commoncrawl.async.EventLoop;

/** 
 * ActorInterface - An actor responds to messages via the ActorInterface
 * 
 * @author rana
 *
 */
public interface ActorInterface {

  public static class RPCSpecification {

    public RPCSpecification(String name, RPCMessageDispatcher dispatcher) {
      _name = name;
      _dispatcher = dispatcher;
    }

    public String               _name;
    public RPCMessageDispatcher _dispatcher;
  }
  
  public static class AsyncStub {

    // channel through which this stub will 
    // communicate with the target actor
    private Channel _channel;
    // optional event loop the stub is running in
    private EventLoop _eventLoop;

    public AsyncStub(Channel channel,EventLoop optionalEventLoop) {
      _channel = channel;
      _eventLoop = optionalEventLoop;
    }

    /**
     * Get the Channel this stub is using to communicate to the Actor 
     * @return Channel
     */
    public Channel getChannel() {
      return _channel;
    }
    
    /**
     * Get the EventLoop this stub is running in
     * @return EventLoop
     */
    public EventLoop getEventLoop() { 
      return _eventLoop;
    }
  }

  public static class BlockingStub<StubType extends AsyncStub> {

    /** default RPC Timeout **/
    private static final int DEFAULT_RPC_TIMEOUT = 60000;

    private StubType         _asyncStub;
    private int              _rpcTimeout         = DEFAULT_RPC_TIMEOUT;

    public StubType getAsyncStub() {
      return _asyncStub;
    }

    public void setRPCTimeout(int milliseconds) {
      _rpcTimeout = milliseconds;
    }

    public int getRPCTimeout() {
      return _rpcTimeout;
    }

    public BlockingStub(StubType asyncStub) {
      _asyncStub = asyncStub;
    }

    protected boolean waitForResult(CountDownLatch latch) throws IOException {

      EventLoop sourceEventLoop = _asyncStub.getEventLoop();
      
      // if called from event thread ...
      if (sourceEventLoop != null && Thread.currentThread() == 
          sourceEventLoop.getEventThread()) {
        // pump events until timeout is reached

        long timeoutTime = System.currentTimeMillis() + getRPCTimeout();

        do {
          sourceEventLoop.waitForIO(1);
        } while (latch.getCount() == 1
            || System.currentTimeMillis() >= timeoutTime);
      } else {
        try {
          latch.await(getRPCTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          // Should never happen ... ?
          // TODO: IGNORE OR THROW RUNTIME EXCEPTION ?
          throw new RuntimeException(e);
        }
      }

      return latch.getCount() == 0;
    }
  }

}
