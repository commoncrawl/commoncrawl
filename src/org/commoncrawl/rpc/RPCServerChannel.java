package org.commoncrawl.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.internal.NIOClientSocket;
import org.commoncrawl.io.internal.NIOClientTCPSocket;
import org.commoncrawl.io.internal.NIOServerSocketListener;
import org.commoncrawl.io.internal.NIOServerTCPSocket;
import org.commoncrawl.io.internal.NIOSocket;

/** 
 * RPCServerChannel - Listens for and routes RPCMessages on a given 
 * Socket/Endpoint.
 * @author rana
 *
 */
public class RPCServerChannel implements NIOServerSocketListener {

  public static interface ConnectionCallback {

    void IncomingClientConnected(RPCChannel channel);

    void IncomingClientDisconnected(RPCChannel channel);

  }

  private static int             INITIAL_RECONNECT_DELAY = 1000;
  InetSocketAddress              _address;
  EventLoop                      _eventLoop;
  NIOServerTCPSocket             _socket;
  Timer                          _reconnectTimer         = null;
  int                            _reconnectDelay         = 0;
  RPCActorService                         _server;
  LinkedList<RPCChannel> _activeClients          = new LinkedList<RPCChannel>();
  ConnectionCallback             _callback;

  private enum State {
    OPEN, OPEN_CONNECTED, CLOSED
  }

  State _state = State.CLOSED;

  public RPCServerChannel(RPCActorService server, EventLoop eventLoop,
      InetSocketAddress address, ConnectionCallback callback) {
    _address = address;
    _eventLoop = eventLoop;
    _server = server;
    _callback = callback;
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

      _eventLoop.setTimer(_reconnectTimer);
    }
  }

  void bind() throws IOException {
    // increate reconnect delay ...
    _reconnectDelay = _reconnectDelay * 2;
    _reconnectTimer = null;

    if (_socket == null) {
      _socket = new NIOServerTCPSocket(this);
      _socket.open(_address);
      _eventLoop.getSelector().registerForAccept(_socket);
      // update state ...
      _state = State.OPEN_CONNECTED;
    }
  }

  void release() {
    // stop accepting sockets on this host ...
    if (_reconnectTimer != null) {
      _eventLoop.cancelTimer(_reconnectTimer);
      _reconnectTimer = null;
    }
    if (_socket != null) {
      _eventLoop.getSelector().cancelRegistration(_socket);
      _socket.close();
      _socket = null;
    }

    for (RPCChannel client : _activeClients) {
      try {
        client.close();
      } catch (IOException e) {

      }
    }
    _activeClients.clear();

    _state = State.CLOSED;
  }

  RPCActorService getServer() {
    return _server;
  }

  // @Override
  public void Accepted(NIOClientSocket newClientSocket) throws IOException {
    RPCChannel newChannel = new RPCChannel(
        (NIOClientTCPSocket) newClientSocket, this);
    _activeClients.add(newChannel);
    if (_callback != null) {
      _callback.IncomingClientConnected(newChannel);
    }
  }

  // @Override
  public void Disconnected(NIOSocket theSocket, Exception disconnectReason)
      throws IOException {

  }

  void ClientChannelDisconnected(RPCChannel channel) {
    if (_callback != null)
      _callback.IncomingClientDisconnected(channel);
    _activeClients.remove(channel);
  }

  final void dispatchRequest(RPCChannel source,RPCFrame.IncomingFrame frame) throws RPCException {
    getServer().dispatchWireRequest(this, source, frame);
  }

  public void Excepted(NIOSocket socket, Exception e) {

  }
}
