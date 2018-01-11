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

public abstract class NIOClientSocket implements NIOSocket {

    boolean _readsDisabled;
    
	  /** connect to the given address and port  */
	  public abstract void  connect(InetSocketAddress address) throws IOException;
	  /** used to finalize the establishment of the connection once the underlying Socket has become connectable*/
	  public abstract boolean finishConnect()throws IOException;
	  
	  public abstract InetSocketAddress getLocalSocketAddress() throws IOException;
	  public abstract InetSocketAddress getSocketAddress()throws IOException;
	  
	  /** read some data from the socket */
	  public abstract int   read(ByteBuffer dst)throws IOException;
	  /** write some data to the socket */
	  public abstract int   write(ByteBuffer dst) throws IOException ;
	  
	  /** return true if reads have been disabled **/
	  synchronized public boolean readsDisabled() { 
	    return _readsDisabled;
	  }
	  
	  synchronized public void disableReads() { 
	    _readsDisabled = true;
	  }
	  
	   synchronized public void enableReads() { 
	     _readsDisabled = false;
	      
	   }

	  
}
