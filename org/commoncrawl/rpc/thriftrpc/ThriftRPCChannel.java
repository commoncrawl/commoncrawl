package org.commoncrawl.rpc.thriftrpc;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TIOStreamTransport;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.internal.NIOBufferList;
import org.commoncrawl.io.internal.NIOBufferListInputStream;
import org.commoncrawl.io.internal.NIOBufferListOutputStream;
import org.commoncrawl.io.internal.NIOClientSocket;
import org.commoncrawl.io.internal.NIOClientSocketListener;
import org.commoncrawl.io.internal.NIOClientTCPSocket;
import org.commoncrawl.io.internal.NIOSocket;
import org.commoncrawl.rpc.RPCChannel;
import org.commoncrawl.rpc.RPCServerChannel;
import org.commoncrawl.util.shared.CCStringUtils;

public class ThriftRPCChannel implements NIOClientSocketListener,
Comparable<ThriftRPCChannel> {
  
  
  public static class AsyncStub { 
    
    private ThriftRPCChannel _channel;
    
    public AsyncStub(ThriftRPCChannel channel) { 
        _channel = channel;
    }
    
    public ThriftRPCChannel getChannel() {
        return _channel;
    }
  }  

  private static long _lastChannelId = 0;

  private long        _channelId     = 0;

  public static interface ConnectionCallback {

    /** called when the Outgoing Channel is connected **/
    void OutgoingChannelConnected(ThriftRPCChannel channel);

    /**
     * called when the Channel is disconnected
     * 
     */
    void OutgoingChannelDisconnected(ThriftRPCChannel channel);
  }

  public static final Log                                  LOG                     = LogFactory.getLog(ThriftRPCChannel.class);

  private static int                                       INITIAL_RECONNECT_DELAY = 1000;
  private static int                                       MAX_RECONNECT_DELAY     = 5000;

  private EventLoop                                        _eventLoop;
  // private String _path;
  private int                                               _lastRequestId          = 0;
  private Map<Integer, ThriftOutgoingMessageContext<TBase, TBase>>    _requestMap             = Collections
  .synchronizedMap(new HashMap<Integer, ThriftOutgoingMessageContext<TBase, TBase>>());

  // list of requests that were already sent
  private LinkedList<ThriftOutgoingMessageContext<TBase, TBase>>      _deferredSendQueue              = new LinkedList<ThriftOutgoingMessageContext<TBase, TBase>>();

  // open for business or not
  private boolean                                          _isOpen                 = false;
  private boolean                                          _isOnline = false;
  private InetSocketAddress                                _localAddress;
  private InetSocketAddress                                _address;
  private NIOClientSocket                                  _socket;
  private int                                              _reconnectDelay         = 0;
  private Timer                                            _reconnectTimer;
  private NIOBufferList                                    _output                 = new NIOBufferList();
  private NIOBufferList                                    _input                  = new NIOBufferList();
  private NIOBufferListInputStream                         _inputStream            = new NIOBufferListInputStream(
      _input);
  private DataInputStream                                  _dataInputStream        = new DataInputStream(_inputStream);
  private NIOBufferListOutputStream                        _outputStream           = new NIOBufferListOutputStream(
      _output);
  
  private TProtocolFactory                                 _outputProtocolFactory;
  private TProtocol                                        _inputProtocol;
  private TProtocol                                        _outputProtocol;

  /** back pointer to server channel is this is an inoming client channel **/
  RPCServerChannel                                       _serverChannel          = null;
  /** connection callback **/
  ConnectionCallback                                     _connectionCallback;   
  
  // in the midst of reading the frame size off the wire
  private static final int READING_FRAME_SIZE = 1;
  // reading the actual frame data now, but not all the way done yet
  private static final int READING_FRAME = 2;
  
  private int                                              _readState = READING_FRAME_SIZE;
  private int                                              _frameSize = 0;

  // constructor
  public ThriftRPCChannel(EventLoop client,
      TProtocolFactory inputProtocolFactory,
      TProtocolFactory outputProtocolFactory,
      InetSocketAddress localAddress, InetSocketAddress address,
      ConnectionCallback callback) throws IOException {

    _inputProtocol = inputProtocolFactory.getProtocol(new TIOStreamTransport(_inputStream));
    _outputProtocolFactory= outputProtocolFactory;
    _outputProtocol = createOutputProtocol();
    
    synchronized (RPCChannel.class) {
      _channelId = ++_lastChannelId;
    }

    _eventLoop = client;
    // _path = servicePath;
    _address = address;
    _localAddress = localAddress;
    _connectionCallback = callback;
  }

  private TProtocol createOutputProtocol() { 
    return _outputProtocolFactory.getProtocol(new TFramedTransport(new TIOStreamTransport(_outputStream)));    
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
      disconnect(_isOnline);
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

    disconnect(_isOnline);

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

  private synchronized void disconnect(boolean generateEvent) {

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
    _output.reset();
    _outputStream.reset();
    _outputProtocol = createOutputProtocol();
    _readState = READING_FRAME_SIZE;
    _frameSize = 0;
    _isOnline = false;
    
    if (generateEvent) { 
      // if incoming channel then a disconnect is the end of the line ...
      cancelOutgoingMessages();
      if (_connectionCallback != null) {
        _connectionCallback.OutgoingChannelDisconnected(this);
      }
    }
  }

  RPCServerChannel getServerChannel() {
    return _serverChannel;
  }

  @SuppressWarnings("unchecked")
  public synchronized void sendRequest(final ThriftOutgoingMessageContext request){

    if (Thread.currentThread() != getEventLoop().getEventThread()) {
      getEventLoop().queueAsyncRunnable(new Runnable() {

        @Override
        public void run() {
          sendRequest(request);
        }
      });
    }
    else  {
      int requestId = 0;
  
      try {
        synchronized (this) {
          requestId = ++_lastRequestId;
        }
  
        request.setRequestId(requestId);
        
        try {
          // if in an open/connected state ... 
          if (_isOnline) {
            encodeRequest(request);
            _requestMap.put(requestId, request);
          } else {
            // add it to the deferred send queue 
            _deferredSendQueue.add(request);
          }
        }
        catch (TException e) { 
          LOG.error(CCStringUtils.stringifyException(e));
          // ok immediately fail request ... 
          request.getCallback().onError(request,e);
        }
  
        if (_socket != null && _socket.isOpen()) {
          getEventLoop().getSelector().registerForReadAndWrite(_socket);
        }
  
      } catch (IOException e) {
  
        _requestMap.remove(requestId);
        _deferredSendQueue.remove(request);
  
        LOG.error("IOException during sendRequest:" + e.toString());
        
        // immediately fail request ... 
        request.getCallback().onError(request,e);
      }
    }
  }

  private void encodeRequest(ThriftOutgoingMessageContext<TBase, TBase> request) throws TException { 
    // start the message output ...
    _outputProtocol.writeMessageBegin(new org.apache.thrift.protocol.TMessage(request.getMethodName(), org.apache.thrift.protocol.TMessageType.CALL, request.getRequestId()));
    // write the request parameters ... 
    request.getInputArgs().write(_outputProtocol);
    // write the trailer ... 
    // end outgoing message ... 
    _outputProtocol.writeMessageEnd();
    // flush 
    _outputProtocol.getTransport().flush();    
  }
 
  public EventLoop getEventLoop() {
    return _eventLoop;
  }

  // @Override
  public synchronized void Connected(NIOClientSocket theSocket)
  throws IOException {
    _isOnline = true;
    _reconnectDelay = INITIAL_RECONNECT_DELAY;
    // clear request map ... 
    _requestMap.clear();
    // if requests are queued up to go ... 
    if (_deferredSendQueue.size() != 0) {
      // swap out lists ....
      LinkedList<ThriftOutgoingMessageContext<TBase, TBase>> temp = _deferredSendQueue;
      _deferredSendQueue = new LinkedList<ThriftOutgoingMessageContext<TBase, TBase>>();
      // and resend all messages ...
      for (ThriftOutgoingMessageContext<TBase, TBase> request : temp) {
        // resed requests 
        sendRequest(request);
      }
      getEventLoop().getSelector().registerForWrite(_socket);
    }
    if (_connectionCallback != null)
      _connectionCallback.OutgoingChannelConnected(this);
  }

  // @Override
  public synchronized void Disconnected(NIOSocket theSocket,
      Exception disconnectReason) throws IOException {
    _isOnline = false;
    // if incoming channel then a disconnect is the end of the line ...
    cancelOutgoingMessages();

    if (_connectionCallback != null) {
      _connectionCallback.OutgoingChannelDisconnected(this);
    }

    // check to see if the channel is still in an OPEN state ...
    // someone may have explicitly CLOSED the channel as a result of the
    // callback ...
    if (isOpen()) {
      reconnect();
    }
  }

  private synchronized void cancelOutgoingMessages() {
    // create temp list ... 
    LinkedList<ThriftOutgoingMessageContext<TBase, TBase>> tempList = new LinkedList<ThriftOutgoingMessageContext<TBase, TBase>>();
    // add all outstanding requests from map 
    tempList.addAll(_requestMap.values());
    // clear request map ... 
    _requestMap.clear();
    // iterate oustanding requests and cancel them 
    for (ThriftOutgoingMessageContext<TBase, TBase> request : tempList) {
      request.getCallback().onError(request,new TException("Request Cancelled"));
    }
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
      // next read incoming frames as appropriate ...
      readResponseFrames();
      // reset the selection state

      // if the output buffer has data that needs to go out ...
      if (_output.isDataAvailable()) {
        getEventLoop().getSelector().registerForReadAndWrite(_socket);
      }
      // otherwise, we may be waiting for response frames ...
      // or this is an incoming socket, in which case we are always in a
      // readable state ...
      else if (_deferredSendQueue.size() != 0) {
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
      } else if (_deferredSendQueue.size() != 0) {
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


    while (_inputStream.available() != 0) {
      
      switch (_readState) { 
        case READING_FRAME_SIZE: {
          // enought to read frame size ? 
          if (_inputStream.available() >= 4) { 
            _frameSize = _dataInputStream.readInt();
            _readState = READING_FRAME;
          }
          else {
            break;
          }
        }
        break;
        
        case READING_FRAME: { 
          if (_inputStream.available() >= _frameSize) { 
            //cache bytes available 
            int bytesOriginallyAvailable = _inputStream.available();
            try { 
              // ok read the message header ... 
              TMessage message = _inputProtocol.readMessageBegin();
              // lookup the request id in the sent map ...
              ThriftOutgoingMessageContext associatedRequest = _requestMap.get(message.seqid);
              // ok if null .. 
              if (associatedRequest != null) { 
                // remove it dude ... 
                _requestMap.remove(message.seqid);
                // remove from resend queue ... 
                _deferredSendQueue.remove(associatedRequest);
                // ok time to read the input data ... 
                associatedRequest.getResultArgs().read(_inputProtocol);
                // and initiate the callback 
                associatedRequest.getCallback().onComplete(associatedRequest);
              }
            }
            catch (TException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
              throw new IOException(e);
            }
            finally {
              // ok we need to skip past remaining bytes ... 
              int bytesRead = bytesOriginallyAvailable - _inputStream.available();
              // skip overflow  
              int bytesToSkip = _frameSize - bytesRead;
              if (bytesToSkip != 0) { 
                LOG.error("Skipping Extra Bytes("+ bytesToSkip + ")!!");
                _inputStream.skip(bytesToSkip);
              }
              
              
              _frameSize = 0;
              _readState = READING_FRAME_SIZE;
            }
          }
          else { 
            break;
          }
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
  public int compareTo(ThriftRPCChannel o) {
    long comparisonResult = this._channelId - o._channelId;
    return (comparisonResult < 0 ? -1 : (comparisonResult > 0) ? 1 : 0);
  }

}
