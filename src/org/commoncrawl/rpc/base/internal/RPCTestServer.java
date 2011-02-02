package org.commoncrawl.rpc.base.internal;

import java.net.InetSocketAddress;

import org.commoncrawl.async.EventLoop;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Callback;
import org.commoncrawl.rpc.base.internal.AsyncRequest.Status;
import org.commoncrawl.rpc.base.shared.RPCException;
import org.junit.Test;

public class RPCTestServer extends Server implements RPCTestService {

  // method hello (in UnitTestStruct1, out UnitTestStruct1);
  public void hello(AsyncContext<UnitTestStruct1, UnitTestStruct1> rpcContext)
      throws RPCException {

    System.out.println("Server:Received Request:"
        + rpcContext.getInput().getIntType());
    rpcContext.getOutput().setStringType(
        rpcContext.getInput().getStringType() + " back");
    rpcContext.getOutput().setIntType(rpcContext.getInput().getIntType());

    rpcContext.completeRequest();
  }

  @Test
  public void testServerRPC() throws Exception {

    final EventLoop eventLoop = new EventLoop();

    eventLoop.start();

    RPCTestServer server = new RPCTestServer();

    InetSocketAddress localAddress = new InetSocketAddress("localhost", 0);

    InetSocketAddress address = new InetSocketAddress("localhost", 9000);

    AsyncServerChannel channel = new AsyncServerChannel(server, eventLoop,
        address, null);

    server.registerService(channel, RPCTestService.spec);

    server.start();

    AsyncClientChannel clientChannel = new AsyncClientChannel(eventLoop,
        localAddress, address, null);

    AsyncStub stub = new AsyncStub(clientChannel);

    UnitTestStruct1 input = new UnitTestStruct1();

    for (int i = 0; i < 1000; ++i) {

      input.setStringType("hello" + Integer.toString(i));
      input.setIntType(i);

      System.out.println("Sending Request:" + i);
      stub.hello(input, new Callback<UnitTestStruct1, UnitTestStruct1>() {

        public void requestComplete(
            AsyncRequest<UnitTestStruct1, UnitTestStruct1> request) {
          System.out.println("Request returned with status:"
              + request.getStatus().toString());

          if (request.getStatus() == Status.Success) {
            System.out.println("Returned string value is:"
                + request.getOutput().getStringType());

            if (request.getOutput().getIntType() == 999) {
              System.out
                  .println("Got Final Response. Stopping Event Loop from within Callback");
              eventLoop.stop();
            }
          }
        }
      });
      System.out.println("Sent Request:" + i);

    }

    // wait for server to quit ...
    eventLoop.getEventThread().join();
  }
}
