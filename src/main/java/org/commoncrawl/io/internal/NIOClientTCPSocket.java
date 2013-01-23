/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.io.internal;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * NIOTCPSocket - TCP version of NIOSocket 
 * 
 * @author rana
 *
 */
public class NIOClientTCPSocket extends NIOClientSocket {
	
  public static final Log LOG = LogFactory.getLog(NIOClientTCPSocket.class);
  
	enum SocketType { 
		/** outgoing socket, initiated by the client **/
		Outgoing,
		/** incoming socket, accepted by the server **/
		Incoming
	}
	
	int					_socketId = NIOSocket._idFactory.getNextId();
	SocketType		_socketType;
	SocketChannel 	_channel;
	NIOClientSocketListener _listener;
	InetSocketAddress _localAddress;
	InetSocketAddress _address;
  
  public NIOClientTCPSocket(InetSocketAddress localAddress,NIOClientSocketListener socketListener) throws IOException { 
    _socketType = SocketType.Outgoing;
    _channel = SocketChannel.open();
    if (localAddress != null) { 
      InetSocketAddress modifiedLocalAddress = new InetSocketAddress(localAddress.getAddress(),0);
      // System.out.println("Binding To Local Address:" + modifiedLocalAddress.getAddress());
      // LOG.info(this + "Binding To Local Address:" + modifiedLocalAddress.getAddress());
      _channel.socket().bind(modifiedLocalAddress);
    }
    _localAddress = (InetSocketAddress) _channel.socket().getLocalSocketAddress();
    setClientSocketOptions(_channel);
    _listener = socketListener;
    
  }

  private static void setClientSocketOptions(SocketChannel channel) throws IOException{ 
    //channel.socket().setPerformancePreferences(0, 1, 3);
    channel.socket().setTcpNoDelay(true);
    //probeAndSetSize(false,2<<18,2<<10,channel);
    //probeAndSetSize(true,2<<15,2<<10,channel);
    channel.configureBlocking(false);
  }
  
  /** internal constructor used to create NIOSocket objects for incoming client connections **/ 
  NIOClientTCPSocket(SocketChannel channel) throws IOException {
	  _socketType = SocketType.Incoming;
	  _channel = channel;
	  _address = new InetSocketAddress(channel.socket().getInetAddress(),channel.socket().getPort());
  }
  
  // @Override 
  public int getSocketId() { 
	  return _socketId;
  }
  
  //@Override
  public boolean isOpen() { 
    return _channel != null;
  }
  
  //@Override
  public void close(){
    
    if (_channel != null) {
      try { 
      _channel.close();
      }
      catch (IOException e) { 
        System.out.println(e);
      }
      _channel = null;
    }
  }

  //@Override
  public void connect(InetSocketAddress address) throws IOException {
    // System.out.println("Connecting to:"+address.getHostAddress()+" at port:"+Integer.toString(port));
	if (_socketType == SocketType.Incoming) { 
		throw new IOException("Invalid State-Connect called on an Incoming (server) Socket");
	}
	_address = address;
	
	if (_channel == null) { 
	  throw new IOException("Invalid State- Channel Not Open during connect call");
	}
	// LOG.info(this + "Connecting to: " + address.getAddress().getHostAddress() + ":" + address.getPort() + " via Interface:" + _channel.socket().getLocalAddress().getHostAddress());
	_channel.connect(address);
  }
  
  //@Override
  public AbstractSelectableChannel getChannel() {
    return _channel;
  }

  //@Override
  public NIOSocketListener getListener() {
    return _listener;
  }
  
  /** set the socket listener for this object */
  public void setListener(NIOSocketListener listener) { 
	  _listener = (NIOClientSocketListener) listener;
  }
  

  //@Override
  public int read(ByteBuffer dst) throws IOException {
    if (_channel == null) { 
      throw new IOException("Invalid State - read called on closed channel");
    }
    return _channel.read(dst);
  }

  //@Override
  public int write(ByteBuffer src) throws IOException {
    if (_channel == null) { 
      throw new IOException("Invalid State - read called on closed channel");
    }
    return _channel.write(src);
  }
  
  //@Override
  public boolean finishConnect()throws IOException { 
		if (_socketType == SocketType.Incoming) { 
			throw new IOException("Invalid State-Connect called on an Incoming (server) Socket");
		}

		if (_channel == null) { 
		  throw new IOException("Invalid State - finishConnect called on closed channel");
		}

		try { 
  		if (_channel.finishConnect()) { 
  			
  		  _channel.socket().setKeepAlive(true);
  			
  		  // LOG.info(this + "Connected to: " + _address.getAddress().getHostAddress() + ":" + _address.getPort() + " via Interface:" + _channel.socket().getLocalAddress().getHostAddress());
  
  			return true;
  		}
		}
		catch (IOException e) { 
		  //LOG.error("channel.finishConnect to address:" + _address.getAddress().getHostAddress() +" port: " + _address.getPort() + " threw exception:" + e.toString());
		  throw e;
		}
		return false;
  }

  //@Override
  public InetSocketAddress getSocketAddress() throws IOException {
  	return _address;
  }
  
  public InetSocketAddress getLocalSocketAddress() throws IOException { 
    if (_channel != null && _channel.isOpen()) { 
      return (InetSocketAddress) _channel.socket().getLocalSocketAddress();
    }
    return _localAddress;
  }
  

}
