package org.commoncrawl.thriftrpc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer.AbstractServerArgs;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.internal.NIOClientSocket;
import org.commoncrawl.io.internal.NIOClientTCPSocket;
import org.commoncrawl.io.internal.NIOServerSocket;
import org.commoncrawl.io.internal.NIOServerSocketListener;
import org.commoncrawl.io.internal.NIOServerTCPSocket;
import org.commoncrawl.io.internal.NIOSocket;
import org.commoncrawl.io.internal.NIOSocketListener;
import org.commoncrawl.thriftrpc.ThriftUnitTest;
import org.commoncrawl.thriftrpc.ThriftUnitTestStruct1;
import org.commoncrawl.thriftrpc.ThriftUnitTest_CCAsyncSupport;
import org.commoncrawl.thriftrpc.ThriftUnitTest.hello2_args;
import org.commoncrawl.thriftrpc.ThriftUnitTest.hello2_result;
import org.commoncrawl.thriftrpc.ThriftUnitTest.hello_args;
import org.commoncrawl.thriftrpc.ThriftUnitTest.hello_result;
import org.commoncrawl.thriftrpc.ThriftUnitTest_CCAsyncSupport.ThriftUnitTest_AsyncCallStub;
import org.commoncrawl.thriftrpc.ThriftUnitTest_CCAsyncSupport.ThriftUnitTest_AsyncIFace;
import org.commoncrawl.thriftrpc.ThriftUnitTest_CCAsyncSupport.ThriftUnitTest_RequestProcessor;
import org.commoncrawl.util.shared.CCStringUtils;

/**
 * A nonblocking TServer implementation. This allows for fairness amongst all
 * connected clients in terms of invocations.
 *
 * This server is inherently single-threaded. If you want a limited thread pool
 * coupled with invocation-fairness, see THsHaServer.
 *
 * To use this server, you MUST use a TFramedTransport at the outermost
 * transport, otherwise this server will be unable to determine when a whole
 * method call has been read off the wire. Clients must also use TFramedTransport.
 */
public class ThriftAsyncServerChannel implements NIOServerSocketListener {
  
  
  public static final Log LOG = LogFactory.getLog(ThriftAsyncServerChannel.class);


  public static class Args extends AbstractNonblockingServerArgs<Args> {
    public Args(TNonblockingServerTransport transport) {
      super(transport);
    }
  }

  public static abstract class AbstractNonblockingServerArgs<T extends AbstractNonblockingServerArgs<T>> extends AbstractServerArgs<T> {
    public long maxReadBufferBytes = Long.MAX_VALUE;

    public AbstractNonblockingServerArgs(TNonblockingServerTransport transport) {
      super(transport);
      transportFactory(new TFramedTransport.Factory());
    }
  }
 

  /**
   * The maximum amount of memory we will allocate to client IO buffers at a
   * time. Without this limit, the server will gladly allocate client buffers
   * right into an out of memory exception, rather than waiting.
   */
  final long MAX_READ_BUFFER_BYTES;

  /**
   * How many bytes are currently allocated to read buffers.
   */
  long readBufferBytesAllocated = 0;
  
 
  final ThriftAsyncRequestProcessor       processor;
  final TProtocolFactory inputProtocolFactory = new TBinaryProtocol.Factory();
  final TProtocolFactory outputProtocolFactory = new TBinaryProtocol.Factory();
  EventLoop              eventLoop;
  boolean                isServing = false;
  NIOServerTCPSocket     _socket;
  InetSocketAddress      _address;
  int                    _reconnectDelay         = 0;
  Timer                  _reconnectTimer         = null;
  private static int             INITIAL_RECONNECT_DELAY = 1000;

  private enum State {
    OPEN, OPEN_CONNECTED, CLOSED
  }

  State _state = State.CLOSED;


  public ThriftAsyncServerChannel(EventLoop eventLoop,InetSocketAddress bindAddress,ThriftAsyncRequestProcessor processor,long max_read_buffer_size) {
    
    MAX_READ_BUFFER_BYTES = max_read_buffer_size;
    this._address = bindAddress;
    this.processor = processor;
    this.eventLoop = eventLoop; 
  }

  public void open() throws IOException {
    if (_state == State.CLOSED) {
      rebind();
    }
  }

  public void close() {
    if (_state != State.CLOSED) {
      release();
    }
  }

  void rebind() throws IOException {

    release();

    if (_reconnectDelay == 0) {
      _reconnectDelay = INITIAL_RECONNECT_DELAY;
      bind();
    } else {
      _reconnectTimer = new Timer(_reconnectDelay, false,

      new Timer.Callback() {

        // @Override
        public void timerFired(Timer timer) {
          try {
            bind();
          } catch (IOException e) {
            e.printStackTrace();
            try {
              rebind();
            } catch (IOException e2) {

            }

          }
        }
      });

      eventLoop.setTimer(_reconnectTimer);
    }
  }

  void bind() throws IOException {
    // increate reconnect delay ...
    _reconnectDelay = _reconnectDelay * 2;
    _reconnectTimer = null;

    if (_socket == null) {
      _socket = new NIOServerTCPSocket(this);
      _socket.open(_address);
      eventLoop.getSelector().registerForAccept(_socket);
      // update state ...
      _state = State.OPEN_CONNECTED;
    }
  }

  void release() {
    // stop accepting sockets on this host ...
    if (_reconnectTimer != null) {
      eventLoop.cancelTimer(_reconnectTimer);
      _reconnectTimer = null;
    }
    if (_socket != null) {
      eventLoop.getSelector().cancelRegistration(_socket);
      _socket.close();
      _socket = null;
    }
    _state = State.CLOSED;
  }
  /**
   * Perform an invocation. This method could behave several different ways
   * - invoke immediately inline, queue for separate execution, etc.
   */
  protected boolean requestInvoke(ThriftAsyncRemoteClientChannel channel)throws TException {
    // invoke on the channel 
    channel.invoke();
    // and prepare for the next frame 
    channel.prepareRead();
    return true;
  }

  /**
   * A ThriftAsyncRemoteClientChannel wants to change its selection preferences, but might not be
   * in the select thread.
   */
  protected void requestSelectInterestChange(final ThriftAsyncRemoteClientChannel buffer) {
    eventLoop.setTimer(new Timer(0, false, new Timer.Callback() {
      
      @Override
      public void timerFired(Timer timer) {
        buffer.changeSelectInterests();
      }
    }));
  }
  

  /**
   * Do connection-close cleanup on a given SelectionKey.
   */
  private void cleanupSelectionkey(SelectionKey key) {
    // remove the records from the two maps
    ThriftAsyncRemoteClientChannel buffer = (ThriftAsyncRemoteClientChannel)key.attachment();
    if (buffer != null) {
      // close the buffer
      buffer.close();
    }
    // cancel the selection key
    key.cancel();
  }

  @Override
  public void Accepted(NIOClientSocket theSocket) throws IOException {
    SelectionKey clientKey = null;
    TNonblockingTransport client = null;
    try {
      // accept the connection
      client = new TNonblockingSocket((SocketChannel)theSocket.getChannel());
      try {
        clientKey = client.registerSelector(eventLoop.getSelector().getSelector(), SelectionKey.OP_READ);
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
        throw new TTransportException(e);
      }

      // add this key to the map
      ThriftAsyncRemoteClientChannel frameBuffer = new ThriftAsyncRemoteClientChannel(this, this,client, clientKey);
      clientKey.attach(frameBuffer);
    } catch (TTransportException tte) {
      // something went wrong accepting.
      LOG.warn("Exception trying to accept!", tte);
      tte.printStackTrace();
      if (clientKey != null) cleanupSelectionkey(clientKey);
      if (client != null) client.close();
    }    
  }

  @Override
  public void Disconnected(NIOSocket theSocket, Exception optionalException)
      throws IOException {
    
  }
  
  @Override
  public void Excepted(NIOSocket socket, Exception e) { 
  }
  

  
  public static void main(String[] args) {

   try {
    final EventLoop eventLoop = new EventLoop();

    ThriftUnitTest_RequestProcessor asyncProcessor = new ThriftUnitTest_RequestProcessor(new ThriftUnitTest_AsyncIFace() {

      @Override
      public void ThriftUnitTest_hello(final ThriftAsyncRemoteCallContext<hello_args, hello_result> context)throws TException {
        System.out.println("Server received hello:" + context.getInput().getParamTwo() + " MessageIdx:" + context.getInput().input.intType);
        //System.out.println("IntType:" + context.getInput().input.intType);
        //System.out.println("LongType:" + context.getInput().input.longType);
        //System.out.println("StringType:" + context.getInput().input.stringType);
        //System.out.println("ListOfStrings:");
        //for (String item : context.getInput().input.listOfStrings) {
        //  System.out.println("\t" + item);
        //}
        eventLoop.setTimer(new Timer((long)(Math.random() * 5),false,new Timer.Callback() {

          @Override
          public void timerFired(Timer timer) {
            // allocate a success object ... 
            context.getOutput().success = new ThriftUnitTestStruct1();
            // increment everything ... 
            context.getOutput().success.intType = context.getInput().input.intType;
            context.getOutput().success.longType = context.getInput().input.longType + 1;
            context.getOutput().success.stringType = Integer.toString(Integer.parseInt(context.getInput().input.stringType) + 1);
            context.getOutput().success.listOfStrings = new ArrayList<String>();
            context.getOutput().success.listOfStrings.addAll(context.getInput().input.listOfStrings);
            
            try {
              context.completeRequest();
            } catch (TException e) {
              e.printStackTrace();
            }            
          } 
          
        }));

      }

      @Override
      public void ThriftUnitTest_hello2(ThriftAsyncRemoteCallContext<hello2_args, hello2_result> context)throws TException {
        System.out.println("Server received hello2");
        context.completeRequest();
      } 
      
    });
    
    Thread childThread[] = new Thread[4];
    
    for (int i=0;i<childThread.length;++i) {
      final int threadIdx = i;
      childThread[i] = new Thread(new Runnable() {
  
        @Override
        public void run() {
          // ok second event loop 
          try {
            final EventLoop childEventLoop = new EventLoop();
            // open channel to remote ... 
            final ThriftAsyncClientChannel channel 
            = new ThriftAsyncClientChannel(childEventLoop,
                new TBinaryProtocol.Factory(),
                new TBinaryProtocol.Factory(),
                new InetSocketAddress(InetAddress.getLocalHost(),0), 
                new InetSocketAddress(InetAddress.getLocalHost(), 9090),
                new ThriftAsyncClientChannel.ConnectionCallback() {
                  int receviedMessageCount = 0;
                  
                  @Override
                  public void OutgoingChannelDisconnected(ThriftAsyncClientChannel channel) {
                    
                  }
                  
                  @Override
                  public void OutgoingChannelConnected(ThriftAsyncClientChannel channel) {
                    ThriftUnitTest_AsyncCallStub stub = new ThriftUnitTest_AsyncCallStub(channel);
                    final int MessageCount = 1000;
                    for (int i=0;i<MessageCount;++i) {
                      int messageIndex = i;
                      //System.out.println("Client Channel Connected");
                      ThriftUnitTestStruct1 struct1 = new ThriftUnitTestStruct1();
                      struct1.intType = messageIndex;
                      struct1.longType = 2;
                      struct1.stringType = "3";
                      struct1.listOfStrings = new ArrayList<String>();
                      struct1.listOfStrings.add("One");
                      struct1.listOfStrings.add("Two");
                      struct1.listOfStrings.add("Three");
                      
                      try {
                        //System.out.println("Client Channel Sending Message");
                        stub.hello(struct1, "hello from thread:" + threadIdx, new ThriftAsyncRequest.ThriftAsyncRequestCallback<hello_args,hello_result>() {
    
                          @Override
                          public void onComplete(ThriftAsyncRequest<hello_args,hello_result> request) {
                            System.out.println("Client Channel:" + threadIdx + "Recvd onComplete for msg:" + request.getResultArgs().success.intType);
                            /*
                            System.out.println("received response");
                            System.out.println("IntType:" + response.success.intType);
                            System.out.println("LongType:" + response.success.longType);
                            System.out.println("StringType:" + response.success.stringType);
                            System.out.println("ListOfStrings:");
                            for (String item : response.success.listOfStrings) {
                              System.out.println("\t" + item);
                            }
                            */
                            if (++receviedMessageCount == MessageCount) { 
                              System.out.println("Received 100 Messaged for Client:" + threadIdx);
                              childEventLoop.stop();
                            }
                          }
    
                          @Override
                          public void onError(ThriftAsyncRequest<hello_args,hello_result> result,Exception exception) {
                            System.out.println("Client Channel Recvd onError");
                            exception.printStackTrace();
                            childEventLoop.stop();
                          }
                        });
                      } catch (TException e) {
                        e.printStackTrace();
                      }
                    }
                  }
                }); 
            
            channel.open();
            childEventLoop.start();
            System.out.println("Waiting for child event loop to die");
            try {
              childEventLoop.getEventThread().join();
            } catch (InterruptedException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
            }
            System.out.println("child event loop died");
            
          } catch (IOException e) {
            e.printStackTrace();
          }
        } 
        
      });
      childThread[i].start();
    }
    System.out.println("Starting Child");
    
    System.out.println("Starting Server");
    ThriftAsyncServerChannel server = new ThriftAsyncServerChannel(eventLoop,new InetSocketAddress(InetAddress.getLocalHost(),9090),asyncProcessor,Long.MAX_VALUE);
    server.open();
    eventLoop.start();
  } catch (IOException e) {
    
    e.printStackTrace();
  } 
  }
}
