package org.commoncrawl.rpc.base.internal;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public interface Service {

  public static class Specification {

    public Specification(String name, RequestDispatcher dispatcher) {
      _name = name;
      _dispatcher = dispatcher;
    }

    public String            _name;
    public RequestDispatcher _dispatcher;
  }

  public static class AsyncStub {

    private AsyncClientChannel _channel;

    public AsyncStub(AsyncClientChannel channel) {
      _channel = channel;
    }

    public AsyncClientChannel getChannel() {
      return _channel;
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

      // if called from event thread ...
      if (Thread.currentThread() == _asyncStub.getChannel().getClient()
          .getEventThread()) {
        // pump events until timeout is reached

        long timeoutTime = System.currentTimeMillis() + getRPCTimeout();

        do {
          _asyncStub.getChannel().getClient().waitForIO(1);
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
