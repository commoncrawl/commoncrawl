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
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NIOServerTCPSocket implements NIOServerSocket {

	int					_socketId = NIOSocket._idFactory.getNextId();
	ServerSocketChannel 		_channel;
	NIOServerSocketListener 	_listener;
	
	public static final Log LOG = LogFactory.getLog(NIOServerTCPSocket.class);
	
	public NIOServerTCPSocket(NIOServerSocketListener listener) { 

		_listener = listener;
	}
	
	// @Override 
	public int getSocketId() { 
	  return _socketId;
	}
	
	//@Override
	public void open(InetSocketAddress address) throws IOException {
		if (_channel != null) { 
			throw new IOException("Invalid State. Socket already bound");
		}
		LOG.info(this + " Binding to: " + address.getAddress().getHostAddress() + ":" + address.getPort());
		_channel = ServerSocketChannel.open();
		setListenerSocketOptions(_channel);
		_channel.socket().bind(address);
	}

	private static void setListenerSocketOptions(ServerSocketChannel channel) throws IOException{ 
		channel.socket().setReuseAddress(true);
		channel.configureBlocking(false);
	}

	private static void setClientSocketOptions(SocketChannel channel) throws IOException{ 
		channel.socket().setTcpNoDelay(true);
		channel.configureBlocking(false);
	}
	
	//@Override
	public void close() {
		try { 
			if (_channel != null) { 
				if (_channel.socket() != null)
					_channel.socket().close();
				_channel.close();
			}
		}
		catch (IOException e) { 
			throw new RuntimeException(e);
		}
		finally {
			_channel = null;
		}
	}

	//@Override
	public AbstractSelectableChannel getChannel() {
		return _channel;
	}

	//@Override
	public NIOSocketListener getListener() {
		return _listener;
	}

	//@Override
	public boolean isOpen() {
		return _channel != null;
	}

	//@Override
	public void acceptable() {
		if (_listener != null) {
			
			SocketChannel newClientChannel =  null;
			try { 
				newClientChannel = _channel.accept();
				// set socket options 
				setClientSocketOptions(newClientChannel);
				// allocate a new NIOClientTCPSocket object ... 
				NIOClientTCPSocket newSocketObj = new NIOClientTCPSocket(newClientChannel);
				// inform the listener of the event
				_listener.Accepted(newSocketObj);
			}
			catch (IOException e) {
				if (newClientChannel != null) {
					try { 
						newClientChannel.close();
					}
					catch (IOException e2) { 
						throw new RuntimeException(e2);
					}
				}
				throw new RuntimeException(e);
			}
		}
		
	}

	//@Override
	public void setListener(NIOSocketListener listener) {
		_listener = (NIOServerSocketListener) listener;		
	}

  @Override
  public boolean readsDisabled() {
    return false;
  }
}
