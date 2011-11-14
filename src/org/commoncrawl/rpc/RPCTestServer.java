package org.commoncrawl.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.commoncrawl.async.Callbacks;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.rpc.InProcessActor.InProcessChannel;
import org.commoncrawl.rpc.MessageData.Status;
import org.commoncrawl.rpc.OutgoingMessageContext.Callback;
import org.commoncrawl.rpc.RPCTestService;
import org.commoncrawl.rpc.UnitTestStruct1;
import org.junit.Test;

public class RPCTestServer extends RPCActorService implements RPCTestService {

  public RPCTestServer() {
    super(null);
  }
  // method hello (in UnitTestStruct1, out UnitTestStruct1);
  public void hello(IncomingMessageContext<UnitTestStruct1, UnitTestStruct1> rpcContext)
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

    RPCServerChannel channel = new RPCServerChannel(server, eventLoop,
        address, null);

    server.bindActor(channel, RPCTestService.spec,server,null);

    server.start();

    RPCChannel clientChannel = new RPCChannel(eventLoop,null,
        localAddress, address, null);

    clientChannel.open();
    
    AsyncStub stub = new AsyncStub(clientChannel,eventLoop);

    UnitTestStruct1 input = new UnitTestStruct1();

    for (int i = 0; i < 1000; ++i) {

      input.setStringType("hello" + Integer.toString(i));
      input.setIntType(i);

      System.out.println("Sending Request:" + i);
      stub.hello(input, new Callback<UnitTestStruct1, UnitTestStruct1>() {

        public void requestComplete(
            OutgoingMessageContext<UnitTestStruct1, UnitTestStruct1> request) {
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
  
  public static void main(String[] args) {
    RPCTestServer server = new RPCTestServer();
    try {
      server.testServerRPC();
    } catch (Exception e2) {
      // TODO Auto-generated catch block
      e2.printStackTrace();
    }
    
    try {
      final EventLoop outerEventLoop = new EventLoop();
      outerEventLoop.start();
      final ThreadPoolExecutor targetExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
      final ThreadPoolExecutor sourceExecutor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
      outerEventLoop.queueAsyncCallback(new Callbacks.Callback() {
        
        @Override
        public void execute() {

          
          try {
            final InProcessActor localActor = RPCTestService.InProcessActorFactory.createInProcessActor(new RPCTestService() {

             int responseCount = 0;
             
             @Override
             public void hello(final IncomingMessageContext<UnitTestStruct1, UnitTestStruct1> rpcContext)throws RPCException {
               System.out.println("Actor Received Hello for Message:" + rpcContext.getInput().getIntType() + " Thread:" + Thread.currentThread().getId());
               outerEventLoop.setTimer(new Timer((long)(Math.random() * 10.0),false,new Timer.Callback() {

                @Override
                public void timerFired(Timer timer) {
                  System.out.println("Actor Processing Delayed Hello Response for Message:" + rpcContext.getInput().getIntType() + " Thread:" + Thread.currentThread().getId());
                  responseCount++;
                  rpcContext.getOutput().setIntType(rpcContext.getInput().getIntType());
                  if (responseCount == 100) { 
                    System.out.println("Hit 100 Responses. Killing Actor Thread");
                    rpcContext.getOutput().setLongType(1);
                    ((InProcessChannel)rpcContext.getChannel()).getActor().stop();
                  }
                  try {
                    rpcContext.completeRequest();
                  } catch (RPCException e) {
                    e.printStackTrace();
                  }
                  
                } 
               }));
               
             } 
             
             },targetExecutor,new InProcessActor.Events() {
              
              @Override
              public void onStartup(InProcessActor actor) {
                System.out.println("OnStartup - ThreadId:" + Thread.currentThread().getId());
              }
              
              @Override
              public void onShutdown(InProcessActor actor) {
                System.out.println("OnShutdown- ThreadId:" + Thread.currentThread().getId());
              }
            });
            
            for (int i=0;i<1000;++i) { 
              Channel localChannel = null;

              if (i % 2 == 0)
                localChannel = localActor.createChannel(outerEventLoop);
              else
                localChannel = localActor.createChannel(sourceExecutor);
              
              RPCTestService.AsyncStub stub = new RPCTestService.AsyncStub(localChannel,outerEventLoop);
              
              UnitTestStruct1 struct1 = new UnitTestStruct1();
              struct1.setIntType(i);
              System.out.println("Sending Request:" + struct1.getIntType() + " From Thread:" + Thread.currentThread().getId());
              stub.hello(struct1,new Callback<UnitTestStruct1, UnitTestStruct1>() {
                public void requestComplete(org.commoncrawl.rpc.OutgoingMessageContext<UnitTestStruct1,UnitTestStruct1> request) {
                  System.out.println("Received Request Complete for Request:" + request.getRequestId() + " Thread:" + Thread.currentThread().getId());
                  
                  if (request.getOutput().getLongType() == 1) { 
                    System.out.println("Shutdown Cmd Received");
                    outerEventLoop.queueAsyncCallback(new Callbacks.Callback() {
                      
                      @Override
                      public void execute() {
                        System.out.println("Killing Outer Event Loop - Thread:" + Thread.currentThread().getId());
                        outerEventLoop.stop();
                        targetExecutor.shutdown();
                        sourceExecutor.shutdown();
                      }
                    });
                  }
                };
              });
            }
            localActor.start();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }

      });
      

      
      
    } catch (IOException e1) {
    }
  }
}
