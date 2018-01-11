package org.commoncrawl.rpc;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.internal.NIOBufferList;
import org.commoncrawl.io.internal.NIOBufferListInputStream;
import org.commoncrawl.io.internal.NIOBufferListOutputStream;
import org.commoncrawl.io.internal.NIOClientSocket;
import org.commoncrawl.io.internal.NIOClientSocketListener;
import org.commoncrawl.io.internal.NIOClientTCPSocket;
import org.commoncrawl.io.internal.NIOSocket;
import org.commoncrawl.util.shared.CCStringUtils;

/** RPCChannel - A remotable version of the Channel abstraction. Used to 
 * communicate with a remote RPCServer.
 * @author rana
 *
 */
public class RPCChannel implements NIOClientSocketListener,
    Comparable<RPCChannel>, Channel {

  private static long _lastChannelId = 0;

  private long        _channelId     = 0;

  public static interface ConnectionCallback {

    /** called when the Outgoing Channel is connected **/
    void OutgoingChannelConnected(RPCChannel channel);

    /**
     * called when the Channel is disconnected
     * 
     * @return true if you want to resend outgoing message, false to clear
     *         (cancel) outgoing messages
     */
    boolean OutgoingChannelDisconnected(RPCChannel channel);
  }

  public static final Log                                  LOG                     = LogFactory
                                                                                       .getLog("org.commoncrawl.rpc.AsyncClientChannel");

  private static int                                       INITIAL_RECONNECT_DELAY = 1000;
  private static int                                       MAX_RECONNECT_DELAY     = 5000;

  // the event loop that is polling this channel's socket 
  private EventLoop                                        _eventLoop;
  // the optional thread pool in which callbacks (responses) from the remote client
  // will run in
  private ThreadPoolExecutor                               _optionalThreadPool;
  // private String _path;
  private int                                              _lastRequestId          = 0;
  private Map<Integer, OutgoingMessageContext<? extends RPCStruct, ? extends RPCStruct>> _requestMap
   = Collections.synchronizedMap(new HashMap<Integer, OutgoingMessageContext<? extends RPCStruct, ? extends RPCStruct>>());

  // list of requests that were already sent
  private LinkedList<OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct>>   
    _sendQueue = new LinkedList<OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct>>();

  // open for business or not
  private boolean                                          _isOpen                 = false;
  private InetSocketAddress                                _localAddress;
  private InetSocketAddress                                _address;
  private NIOClientSocket                                  _socket;
  private int                                              _reconnectDelay         = 0;
  private Timer                                            _reconnectTimer;
  private NIOBufferList                                    _output                 = new NIOBufferList();
  private NIOBufferList                                    _input                  = new NIOBufferList();
  private NIOBufferListInputStream                         _inputStream            = new NIOBufferListInputStream(
                                                                                       _input);
  private NIOBufferListOutputStream                        _outputStream           = new NIOBufferListOutputStream(
                                                                                       _output);

  RPCFrame.Decoder                                         _decoder                = new RPCFrame.Decoder(
                                                                                       _inputStream);
  RPCFrame.Encoder                                         _encoder                = new RPCFrame.Encoder(
                                                                                       _outputStream);

  /** back pointer to server channel is this is an incoming client channel **/
  RPCServerChannel                                          _serverChannel          = null;
  /** connection callback **/
  ConnectionCallback                                       _connectionCallback;                                                                                      ;
  /** context object **/
  Object _context;
  
  /**
   * Construct an RPCChannel that talks to a remote server 
   * 
   * @param eventLoop
   * @param localAddress
   * @param address
   * @param callback
   * @throws IOException
   */
  public RPCChannel(EventLoop eventLoop, ThreadPoolExecutor optionalThreadPool,
      InetSocketAddress localAddress,
      InetSocketAddress address, ConnectionCallback callback)
      throws IOException {

    synchronized (RPCChannel.class) {
      _channelId = ++_lastChannelId;
    }

    _eventLoop = eventLoop;
    _optionalThreadPool = optionalThreadPool;
    // _path = servicePath;
    _address = address;
    _localAddress = localAddress;
    _connectionCallback = callback;
  }

  /** 
   * Internal Constructor for creating a RPCChannel on the server side
   * 
   * @param socket
   * @param serverChannel
   * @throws IOException
   */
  RPCChannel(NIOClientTCPSocket socket,
      RPCServerChannel serverChannel) throws IOException {

    synchronized (RPCChannel.class) {
      _channelId = ++_lastChannelId;
    }

    // incoming channels are technically always in an open state ...
    _isOpen = true;
    _serverChannel = serverChannel;
    _socket = socket;
    _eventLoop = _serverChannel._eventLoop;
    _address = socket.getSocketAddress();

    // setup the listener relationship
    socket.setListener(this);
    // register for an initial read on the socket ...
    _eventLoop.getSelector().registerForRead(_socket);
  }
  
  /** set a context object **/
  public void setContext(Object contextObj) { 
    _context = contextObj;
  }
  
  /** get the context object **/
  public Object getContext() { 
    return _context;
  }
  
  
  public synchronized void open() throws IOException {
    if (!_isOpen) {
      _isOpen = true;
      reconnect();
    } else {
      LOG.error("open called on already open channel");
      throw new IOException("Channel Alread Open");
    }
  }

  public synchronized void close() throws IOException {
    if (_isOpen) {
      _isOpen = false;
      disconnect(true);
    }
  }

  public String toString() {
    if (_address != null) {
      return "RPCChannel(" + _address.toString() + ")";
    } else {
      return "Uninitialized ClientRPCChannel";
    }
  }

  /**
   * is this channel in an OPEN state - I.E. was open called on the channel ?
   * NOT to be confused with being in a CONNECTED state.
   * **/
  public synchronized boolean isOpen() {
    return _isOpen;
  }

  public synchronized void reconnect() throws IOException {

    disconnect(false);

    // if reconnect delay is zero (bootstrap stage only)
    // then connect immediately...
    if (_reconnectDelay == 0) {
      _reconnectDelay = INITIAL_RECONNECT_DELAY;
      connect();
    }
    // otherwise schedule a timer base on current reconnect interval
    else {
      _reconnectTimer = new Timer(_reconnectDelay, false,

      new Timer.Callback() {

        // @Override
        public void timerFired(Timer timer) {
          try {
            // LOG.debug("Reconnecting to:"+_address);
            connect();
          } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Reconnect threw exception:" + e.toString());
          }
        }
      });
      // register the timer
      _eventLoop.setTimer(_reconnectTimer);
    }
    // either way, increase subsequent reconnect interval
    _reconnectDelay = Math.min(MAX_RECONNECT_DELAY, _reconnectDelay * 2);
  }

  private synchronized void connect() throws IOException {

    _reconnectTimer = null;
    _socket = new NIOClientTCPSocket(this._localAddress, this);
    _socket.connect(_address);

    getEventLoop().getSelector().registerForConnect(_socket);

  }

  private synchronized void disconnect(boolean flushQueues) {

    if (_reconnectTimer != null) {
      _eventLoop.cancelTimer(_reconnectTimer);
      _reconnectTimer = null;
    }

    // disconnect the underlying socket
    if (_socket != null) {
      _eventLoop.getSelector().cancelRegistration(_socket);
      _socket.close();
      _socket = null;
    }

    _output.reset();
    _input.reset();
    _inputStream.reset();
    _decoder.reset();
    _output.reset();
    _outputStream.reset();
    _encoder.reset();

    if (flushQueues) {
      cancelOutgoingMessages();
    }
    _requestMap.clear();
  }

  synchronized boolean isIncomingChannel() {
    return (_serverChannel != null);
  }

  RPCServerChannel getServerChannel() {
    return _serverChannel;
  }

  @Override
  public synchronized void sendRequest(OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct> request)
      throws RPCException {

    int requestId = 0;

    try {
      synchronized (this) {
        requestId = ++_lastRequestId;
      }

      request.setRequestId(requestId);

      _requestMap.put(requestId, request);

      _encoder.encodeRequest(request);
      _sendQueue.add(request);

      if (_socket != null && _socket.isOpen()) {
        getEventLoop().getSelector().registerForReadAndWrite(_socket);
      }

    } catch (IOException e) {

      _requestMap.remove(requestId);

      e.printStackTrace();

      LOG.error("IOException during sendRequest:" + e.toString());

      throw new RPCException(e);
    }
  }

  @Override
  public synchronized void sendResponse(IncomingMessageContext<? extends RPCStruct,? extends RPCStruct> context)
      throws RPCException {

    // LOG.info("Sending Response");
    if (_socket == null || !_socket.isOpen()) {
      LOG.error("sendResponse invoked on closed channel");
      throw new RPCException("Invoking RPC Response on Closed Channel.");
    }

    try {
      _encoder.encodeResponse(context);
      getEventLoop().getSelector().registerForReadAndWrite(_socket);

    } catch (IOException e) {
      LOG.error("IOException during encodeResponse in sendResponse::"
          + e.toString());
      e.printStackTrace();
      throw new RPCException(e);
    }
  }

  public EventLoop getEventLoop() {
    return _eventLoop;
  }

  // @Override
  @SuppressWarnings("unchecked")
  public synchronized void Connected(NIOClientSocket theSocket)
      throws IOException {

    _reconnectDelay = INITIAL_RECONNECT_DELAY;

    if (_sendQueue.size() != 0) {

      // swap out lists ....
      LinkedList<OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct>> temp = _sendQueue;
      _sendQueue = new LinkedList<OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct>>();
      // and resend all messages ...
      for (OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct> request : temp) {
        try {
          sendRequest(request);
        } catch (RPCException e) {
          LOG.error(e);
          // fail this request ...
          if (request.getCallback() != null) {
            request.setStatus(MessageData.Status.Error_RPCFailed);
            request.setErrorDesc("RPC Failed During Resend");
            request.getCallback().requestComplete(request);
          }
        }
      }

      getEventLoop().getSelector().registerForWrite(_socket);
    }
    if (_connectionCallback != null)
        _connectionCallback.OutgoingChannelConnected(this);
  }

  // @Override
  public synchronized void Disconnected(NIOSocket theSocket,
      Exception disconnectReason) throws IOException {
    // if incoming channel then a disconnect is the end of the line ...
    if (isIncomingChannel()) {
      // close the socket completely ...
      disconnect(true);
      // and notify server channel (parent)
      getServerChannel().ClientChannelDisconnected(this);
    }
    // on the other hand, if this is an outgoing channel, attempt to reconnect
    // to the target...
    else {

      if (_connectionCallback != null) {
        if (_connectionCallback.OutgoingChannelDisconnected(this) == false) {
          cancelOutgoingMessages();
        }
      }

      // check to see if the channel is still in an OPEN state ...
      // someone may have explicitly CLOSED the channel as a result of the
      // callback ...
      if (isOpen()) {
        reconnect();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private synchronized void cancelOutgoingMessages() {

    LinkedList<OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct>> tempList 
      = new LinkedList<OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct>>();

    tempList.addAll(_sendQueue);

    _sendQueue.clear();

    for (final OutgoingMessageContext<? extends RPCStruct,? extends RPCStruct> request : tempList) {
      request.setStatus(OutgoingMessageContext.Status.Error_RPCFailed);
      if (request.getCallback() != null) {
        if (_optionalThreadPool != null) { 
          // initiate callback via thread pool thread
          _optionalThreadPool.submit(new Runnable()  {

            @Override
            public void run() {
              request.getCallback().requestComplete(request);
            } 
            
          });
        }
        else {
          // initiate callback from within async thread 
          request.getCallback().requestComplete(request);
        }
      }
    }
    _sendQueue.clear();
  }

  ByteBuffer readBufferDirect = ByteBuffer.allocateDirect(8096 * 4);

  // @Override
  public synchronized int Readable(NIOClientSocket theSocket)
      throws IOException {

    if (!_socket.isOpen()) {
      LOG.warn("Readable callback called on closed socket");
      return -1;
    }

    int totalBytesRead = 0;

    try {
      // first read everything we can from the socket
      int singleReadAmount = 0;

      do {

        ByteBuffer buffer = _input.getWriteBuf();

        singleReadAmount = _socket.read(buffer);

        /*
         * if (buffer.isDirect()) { singleReadAmount = _socket.read(buffer); }
         * else { readBufferDirect.clear();
         * readBufferDirect.limit(Math.min(buffer
         * .remaining(),readBufferDirect.limit())); singleReadAmount =
         * _socket.read(readBufferDirect); readBufferDirect.flip(); if
         * (singleReadAmount > 0) { buffer.put(readBufferDirect); } }
         */

        if (singleReadAmount > 0) {
          _input.write(buffer);
          totalBytesRead += singleReadAmount;
        }

      } while (singleReadAmount > 0);

      if (totalBytesRead != 0) {
        _input.flush();
      }
      // if this is an outgoing channel, then read response frames ...
      if (!isIncomingChannel()) {
        // next read incoming frames as appropriate ...
        readResponseFrames();
      } else {
        readRequestFrames();
      }

      ;
      // reset the selection state

      // if the output buffer has data that needs to go out ...
      if (_output.isDataAvailable()) {
        getEventLoop().getSelector().registerForReadAndWrite(_socket);
      }
      // otherwise, we may be waiting for response frames ...
      // or this is an incoming socket, in which case we are always in a
      // readable state ...
      else if (_sendQueue.size() != 0 || isIncomingChannel()) {
        // if so, make socket readable ...
        getEventLoop().getSelector().registerForRead(_socket);
      }
    } catch (IOException e) {
      LOG.error("IOException in Readable callback:"
          + CCStringUtils.stringifyException(e));
      e.printStackTrace();
      reconnect();
    }

    return (totalBytesRead == 0) ? -1 : totalBytesRead;
  }

  ByteBuffer writeBufferDirect = ByteBuffer.allocateDirect(8096 * 4);

  // @Override
  public synchronized void Writeable(NIOClientSocket theSocket)
      throws IOException {
    if (!_socket.isOpen()) {
      LOG.warn("Writeable callback called on closed socket");
      return;
    }

    int amountWritten = 0;

    try {

      do {

        amountWritten = 0;

        ByteBuffer bufferToWrite = _output.read();

        if (bufferToWrite != null) {

          try {

            amountWritten = _socket.write(bufferToWrite);
            /*
             * if (bufferToWrite.isDirect()) { amountWritten =
             * _socket.write(bufferToWrite); } else { writeBufferDirect.clear();
             * bufferToWrite.mark(); writeBufferDirect.put(bufferToWrite);
             * writeBufferDirect.flip(); amountWritten =
             * _socket.write(writeBufferDirect); bufferToWrite.reset();
             * bufferToWrite.position(bufferToWrite.position() + amountWritten);
             * }
             */

          } catch (IOException e) {
            throw e;
          }

          if (bufferToWrite.remaining() > 0) {
            _output.putBack(bufferToWrite);
            break;
          }
        }
      } while (amountWritten > 0);

      if (_output.isDataAvailable()) {
        getEventLoop().getSelector().registerForReadAndWrite(_socket);
      } else if (_sendQueue.size() != 0 || isIncomingChannel()) {
        getEventLoop().getSelector().registerForRead(_socket);
      }
    } catch (IOException e) {
      LOG.error("IOException in Writeable callback:" + e.toString());
      reconnect();
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private synchronized void readResponseFrames() throws IOException {

    RPCFrame.IncomingFrame incoming = null;

    while ((incoming = _decoder.getNextResponseFrame()) != null) {

      // lookup the request id in the sent map ...
      final OutgoingMessageContext associatedRequest = _requestMap.get(incoming._requestId);

      if (associatedRequest != null) {

        _requestMap.remove(incoming._requestId);

        synchronized (_sendQueue) {
          _sendQueue.remove(associatedRequest);
        }

        try {

          // set status based on incoming stastus code ...
          associatedRequest
              .setStatus(OutgoingMessageContext.Status.values()[incoming._status]);
          // and if success ... retrieve payload ...

          if (associatedRequest.getStatus() == OutgoingMessageContext.Status.Success) {
            // if found, deserialize the payload into the message object
            associatedRequest.getOutput().deserialize(
                new DataInputStream(incoming._payload), new BinaryProtocol());
          }
          // and if server error ...
          else if (associatedRequest.getStatus() == OutgoingMessageContext.Status.Error_ServerError) {
            // attempt to read error desc if present ...
            if (incoming._payload.available() != 0) {
              associatedRequest.setErrorDesc((new DataInputStream(
                  incoming._payload)).readUTF());
            }
          }

        } catch (IOException e) {
          LOG.error("IOException in readResponseFrame:"
              + CCStringUtils.stringifyException(e));
          associatedRequest.setStatus(OutgoingMessageContext.Status.Error_RPCFailed);
        }

        // and initiate the callback
        if (associatedRequest.getCallback() != null) {
          if (_optionalThreadPool != null) { 
            // initiate callback via thread pool
            _optionalThreadPool.submit(new Runnable() {

              @Override
              public void run() {
                associatedRequest.getCallback().requestComplete(associatedRequest);
              } 
            });
          }
          else {
            // initiate callback in event thread ..
            associatedRequest.getCallback().requestComplete(associatedRequest);
          }
        }
      } else {
        LOG.error("Orphaned request found in readResponseFrame");
      }

    }
  }

  private synchronized void readRequestFrames() throws IOException {

    RPCFrame.IncomingFrame incoming = null;

    while ((incoming = _decoder.getNextRequestFrame()) != null) {

      try {
        getServerChannel().dispatchRequest(this, incoming);
      } catch (RPCException e) {
        LOG.error("RPCException thrown during dispatchRequest:" + e.toString());
        e.printStackTrace();
        // if an RPC Exception is thrown at this point, we need to immediately
        // fail this request ...
        IncomingMessageContext<RPCStruct, RPCStruct> dummyContext = new IncomingMessageContext<RPCStruct, RPCStruct>(
            this,incoming._requestId, null, null);

        dummyContext.setStatus(MessageData.Status.Error_RPCFailed);
        dummyContext.setErrorDesc(e.toString());
        try {
          sendResponse(dummyContext);
        } catch (RPCException e1) {
          LOG.error("RPCException while sending <FAILED> response::"
              + e.toString());
          e.printStackTrace();
        }
      }
    }
  }

  public synchronized void Excepted(NIOSocket socket, Exception e) {
    LOG.error("Runtime Error on Socket:" + StringUtils.stringifyException(e));
    try {
      Disconnected(socket, e);
    } catch (IOException e2) {
      LOG.error(StringUtils.stringifyException(e2));
    }
  }

  @Override
  public int compareTo(RPCChannel o) {
    long comparisonResult = this._channelId - o._channelId;
    return (comparisonResult < 0 ? -1 : (comparisonResult > 0) ? 1 : 0);
  }

}
