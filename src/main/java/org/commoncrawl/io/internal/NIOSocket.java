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

import java.nio.channels.spi.AbstractSelectableChannel;

/** 
 * 
 * @author rana
 * 
 * NIOSocket - Abstract interface used to represent an underlying asynchronous socket. Used primarily to 
 * abstract away from TCP Socket so that we can support HTTPS in the future ... 
 *
 */
public interface NIOSocket {
  
  static class IdFactory { 
	  
	  static int _lastId = 0;
	  
	  synchronized int getNextId() {
		  return ++_lastId;
	  }
  }
  
  static IdFactory _idFactory = new IdFactory();
	
  /** get this socket's unique identifier **/
  int getSocketId();
	
  /** get the NIO channel associated with this socket */ 
  AbstractSelectableChannel getChannel();
  /** get the socket listener for this object */
  NIOSocketListener         getListener();
  /** set the socket listener for this object */
  void setListener(NIOSocketListener listener);

  /** returns true if the socket is valid and connected */
  boolean isOpen();
  /** closes (and disconnects) the socket */
  void  close();
  
  boolean readsDisabled();
  
}
