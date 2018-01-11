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
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
//import org.apache.thrift.TException;
//import org.apache.thrift.async.TAsyncClient;
//import org.apache.thrift.async.TAsyncMethodCall;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.util.shared.CCStringUtils;

/** 
 * NIOSocketSelector - class used to poll a set of asynchronous NIOSocket sockets.
 * 
 * @author rana
 *
 */

public class NIOSocketSelector {

  Selector _selector = null;
  EventLoop _eventLoop = null;

  public static final Log LOG = LogFactory.getLog("org.commoncrawl.io.NIOSocketSelector");
  
  //private final TreeSet<TAsyncMethodCall> timeoutWatchSet = new TreeSet<TAsyncMethodCall>(new TAsyncMethodCallTimeoutComparator());
  //private final ConcurrentLinkedQueue<TAsyncMethodCall> pendingCalls = new ConcurrentLinkedQueue<TAsyncMethodCall>();
  
  
  private static class PendingRegistration { 
	  NIOSocket _socket;
	  int 			_interestOps;
	  
	  PendingRegistration(NIOSocket socket,int socketOp) {
		  _socket = socket;
		  _interestOps = socketOp;
	  }
	  
	  public int getInterestOps() { 
		  return _interestOps;
	  }
	  
	  public void setInterestOps(int ops) {
		  _interestOps = ops;
	  }
	  
	  public NIOSocket getSocket() { 
		  return _socket;
	  }
  }
  
  Map<Integer,PendingRegistration> _pendingRegistrations = new TreeMap<Integer,PendingRegistration>();
  
  public Selector getSelector() {
    return _selector;
  }
  
  /** Constructor - creates a new NIO Selector */
  public NIOSocketSelector(EventLoop eventLoop) throws IOException { 
	 _eventLoop = eventLoop;
    _selector = Selector.open();
  }
  
  /*
  public void call(TAsyncMethodCall method)throws TException { 
    method.prepareMethodCall();
    pendingCalls.add(method);
    try {
      wakeup();
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      throw new TException(e);
    }
  }
  */
  
  /** register a socket for CONNECT events */
  public void registerForConnect(NIOClientSocket theSocket)throws IOException  {
    registerSocket(theSocket,SelectionKey.OP_CONNECT);
  }
  
  /** register a socket for READ events */
  public void registerForRead(NIOClientSocket theSocket)throws IOException { 
    registerSocket(theSocket,SelectionKey.OP_READ);
  }

  /** register a socket for WRITE events */
  public void registerForWrite(NIOClientSocket theSocket)throws IOException { 
    registerSocket(theSocket,SelectionKey.OP_WRITE);
  }
  
  /** register a socket for READ AND WRITE events */
  public void registerForReadAndWrite(NIOClientSocket theSocket)throws IOException { 
    registerSocket(theSocket,SelectionKey.OP_READ|SelectionKey.OP_WRITE);
  }
  
  /** register a socket for ACCEPT events */
  public void registerForAccept(NIOServerSocket theSocket)throws IOException { 
    registerSocket(theSocket,SelectionKey.OP_ACCEPT);
  }
  
  public void enableReads(NIOClientSocket theSocket)throws IOException { 
    
  }
  
  
  /** cancel all event registrations on the specified object (socket) */ 
  public void cancelRegistration(NIOSocket theSocket) { 
    
	if (_eventLoop == null || _eventLoop.getEventThread() == Thread.currentThread()) { 
	    if (theSocket.getChannel() != null) { 
	      SelectionKey key = theSocket.getChannel().keyFor(_selector);
	    
	      if (key != null) { 
	        key.cancel();
	      }
	    }
	}
	else {
		synchronized(_pendingRegistrations) {
			_pendingRegistrations.put(theSocket.getSocketId(),new PendingRegistration(theSocket,0));
		}
		_eventLoop.wakeup();		
	}
  }
    
  /** internal registration helper */
  public void registerSocket(NIOSocket theSocket, int interestOps)throws IOException { 
    
  	if (_eventLoop == null || _eventLoop.getEventThread() == Thread.currentThread()) { 

  	    
  	    SelectionKey key = theSocket.getChannel().keyFor(_selector);
  	    
      if (key == null) {
         if (theSocket.readsDisabled()) { 
           interestOps = (interestOps & ~SelectionKey.OP_READ);
         }
         if (interestOps != 0) { 
           key = theSocket.getChannel().register(_selector,interestOps,theSocket);
         }
      }
      else {
        key.interestOps(key.interestOps() | interestOps);
        if (theSocket.readsDisabled()) { 
          key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        }
      }
  	}
  	else { 
  		synchronized(_pendingRegistrations) {
  			
  			PendingRegistration pendingRegistration = _pendingRegistrations.get(theSocket.getSocketId());
  			if (pendingRegistration == null) { 
  				_pendingRegistrations.put(theSocket.getSocketId(),new PendingRegistration(theSocket,interestOps));
  			}
  			else { 
  				pendingRegistration.setInterestOps(pendingRegistration.getInterestOps() | interestOps);
  			}
  		}
  		_eventLoop.wakeup();
  	}
  }
  
  private final void processPendingRegistrations() { 
	  synchronized(_pendingRegistrations) { 
		  if (_pendingRegistrations.size() != 0) { 
			  for (PendingRegistration registration : _pendingRegistrations.values()) { 
				  if (registration.getInterestOps() == 0) { 
					  cancelRegistration(registration.getSocket());
				  }
				  else {
					  try { 
						  registerSocket(registration.getSocket(),registration.getInterestOps());
					  }
					  catch (IOException e) { 
						  LOG.error("registerSocket threw Exception:" + e.getMessage());
					  }
				  }
			  }
			  _pendingRegistrations.clear();
		  }
	  }
  }
  
  public static class TimeUsageDetail  { 
    public long blockedTime;
    public long unblockedTime;
    public long timeInConnectedEvt;
    public long timeInReadableEvt;
    public long timeInWritableEvt;
    
    void reset() { 

      blockedTime =0;
      unblockedTime = 0;
      timeInConnectedEvt = 0;
      timeInReadableEvt =0;
      timeInWritableEvt =0;
      
    }
  }
  
  
  private long _lastPollTime = -1;
  /** poll method  - poll the registered socket for events and potentially block for IO for the 
   *  specified timeout value 
   *  
   *  @param timeoutValue - amount of time in MS to wait (block) for IO 
   *  
   *  */
  @SuppressWarnings("unchecked")
  public int poll(long timeoutValue,TimeUsageDetail timeUsageDetailOut) throws IOException {
   
    long timeStart = System.currentTimeMillis();
    
    if (_lastPollTime != -1 && (timeStart - _lastPollTime) >= 30000) { 
      LOG.error("POLL Delta Too Long:" + (timeStart - _lastPollTime));
    }
    _lastPollTime = timeStart;
    
    if (timeUsageDetailOut != null) { 
      timeUsageDetailOut.blockedTime = 0;
      timeUsageDetailOut.unblockedTime = 0;
    }
    
    if (_selector == null || !_selector.isOpen()) { 
      IOException e = new IOException("Selector NULL or Selector is Not Open!");
      LOG.error(e);
      throw e;
    }
    
    
    processPendingRegistrations();
    long timeEnd = System.currentTimeMillis();
    
    if (timeUsageDetailOut != null) { 
      timeUsageDetailOut.unblockedTime += (timeEnd - timeStart);
    }
    
    /*
    if (timeoutWatchSet.size() != 0) { 
      // We have a timeout pending, so calculate the time until then and select appropriately
      long nextTimeout = timeoutWatchSet.first().getTimeoutTimestamp();
      long selectTime = nextTimeout - System.currentTimeMillis();
      if (selectTime < timeoutValue) {
        timeoutValue = Math.max(selectTime,0);
      }
    }
    */
    timeStart = System.currentTimeMillis();
    
    int count = 0;
    if (timeoutValue <= 0) {
      count = _selector.selectNow();
    }
    else {
      if (timeoutValue == Long.MAX_VALUE)
        timeoutValue = 0;
      count = _selector.select(timeoutValue);
    }
    timeEnd = System.currentTimeMillis();
    
    if (timeUsageDetailOut != null) { 
      timeUsageDetailOut.blockedTime += (timeEnd - timeStart);
    }


    long unblockedTimeStart = System.currentTimeMillis();
    
    // if (count != 0 ) { 
      
   
      Set<SelectionKey> selectionSet = _selector.selectedKeys();
      
      for (Iterator<SelectionKey> i = selectionSet.iterator(); i.hasNext();) {  
        
        SelectionKey selectionKey = i.next();
        
        i.remove();
        
        if (selectionKey.isValid()) { 

          Object attachment = selectionKey.attachment();
          /*
          if (attachment instanceof TAsyncMethodCall) {
            transitionThriftMethod((TAsyncMethodCall)attachment,selectionKey);
          }
          */
          if (attachment instanceof NIOSocket) {
            NIOSocket theSocket = (NIOSocket) selectionKey.attachment();
            
            if (theSocket != null && theSocket.getListener() != null) { 
          	
          	// reset interest ops 
              selectionKey.interestOps(0);
              
              // process events in key ... 
              if (selectionKey.isConnectable()) {
              	
                boolean connected = false;
                Exception disconnectException = null;
                try {
                  if (((NIOClientSocket)theSocket).finishConnect()) {
                     connected = true;
                     // log it ... 
                     // LOG.info("Connected to:"+((NIOClientSocket)theSocket).getSocketAddress());
                    // reset the selection key's ops.. otherwise, select keeps returning on an already connected socket (since we have registered for CONNECT)
                     System.out.println("Connected to:"+((NIOClientSocket)theSocket).getSocketAddress());
                     timeStart = System.currentTimeMillis();
                    ((NIOClientSocketListener)theSocket.getListener()).Connected((NIOClientSocket)theSocket);
                    if (timeUsageDetailOut != null) { 
                      timeUsageDetailOut.timeInConnectedEvt += System.currentTimeMillis() - timeStart;
                    }
                  }
                  else { 
                    //LOG.error("Failed to Connect to:"+((NIOClientSocket)theSocket).getSocketAddress() + " - finishConnect returned false");
                    theSocket.close();
                  }
                }
                catch (IOException e) {
                  //LOG.error("Failed to Connect to:"+((NIOClientSocket)theSocket).getSocketAddress() + " with Exception:"+e);
                  theSocket.close();  
                  disconnectException = e;
                }
                catch (RuntimeException e) { 
                  LOG.error("Caught Runtime Exception in Connected Event:"+StringUtils.stringifyException(e));
                  ((NIOClientSocketListener)theSocket.getListener()).Excepted((NIOClientSocket)theSocket,e);
                  theSocket.close();
                  disconnectException = e;
                  //KILL THE SERVER 
                  //throw e;
                }
                
                // if we were unable to properly establish the connection, trigger the Disconnected notification ... 
                if (!connected) { 
                  //LOG.error("Failed to Complete Connection in Finish Connect- Calling Disconnected");
              	  ((NIOClientSocketListener)theSocket.getListener()).Disconnected((NIOClientSocket)theSocket,disconnectException);
              	  // continue to the next socket ... 
              	  continue;
                }
              }
              
              // now always set the socket to readable state ... 
              if ((theSocket instanceof NIOClientSocket) && selectionKey.isValid()) { 
              	selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_READ);
              }
              
              if (selectionKey.isValid() && selectionKey.isReadable()) {
              	int bytesRead = -1;
              	
              	try {
              	  //long nanoTimeStart = System.nanoTime();
              	  timeStart = System.currentTimeMillis();
              	  // track the number of actual bytes read in the callback ... 
              	  bytesRead = ((NIOClientSocketListener)theSocket.getListener()).Readable((NIOClientSocket)theSocket);
              	  //long nanoTimeEnd = System.nanoTime();
              	  //System.out.println("Readable Took:" + (nanoTimeEnd - nanoTimeStart) + " BytesRead:" + bytesRead);
                  if (timeUsageDetailOut != null) { 
                    timeUsageDetailOut.timeInReadableEvt += System.currentTimeMillis() - timeStart;
                  }
  
                  if (bytesRead == -1 ) {
                    // log it ... 
                    // LOG.error("Abnormal Disconnect Detected on Socket:"+ ((NIOClientSocket)theSocket).getSocketAddress());
                    // trigger a disconnect event ...
                    ((NIOClientSocketListener)theSocket.getListener()).Disconnected((NIOClientSocket)theSocket,null);
                    // close the socket ... 
                    theSocket.close();
                  }
              	}
                catch (RuntimeException e) { 
                  LOG.error("Caught Runtime Exception in Readable Event:"+StringUtils.stringifyException(e));
                  ((NIOClientSocketListener)theSocket.getListener()).Excepted((NIOClientSocket)theSocket,e);
                  theSocket.close();
                  //KILL THE SERVER 
                  // throw e;
                }
              	// if bytesRead == -1 then this means that the underlying connection has gone bad ... 
              }
              
              if (selectionKey.isValid() && selectionKey.isWritable()) { 
                try { 
                  
                  timeStart = System.currentTimeMillis();
                  ((NIOClientSocketListener)theSocket.getListener()).Writeable((NIOClientSocket)theSocket);
                  // System.out.println("Writable Took:" + (System.currentTimeMillis() - timeStart));
                  if (timeUsageDetailOut != null) { 
                    timeUsageDetailOut.timeInWritableEvt += System.currentTimeMillis() - timeStart;
                  }
                }
                catch (RuntimeException e) { 
                  LOG.error("Caught Runtime Exception in Readable Event:"+StringUtils.stringifyException(e));
                  ((NIOClientSocketListener)theSocket.getListener()).Excepted((NIOClientSocket)theSocket,e);
                  theSocket.close();
                  //KILL THE SERVER ? 
                  //throw e;
                }
                
              }
              
              if (selectionKey.isValid() && selectionKey.isAcceptable()) {
              	((NIOServerSocket)theSocket).acceptable();
              	// re-register for accept on this socket 
              	selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_ACCEPT);
              }
            }
          }
          // exernally managed socket (thrift client socket)
          else if (attachment instanceof NIOClientSocketListener) {
            NIOClientSocketListener listener = (NIOClientSocketListener) attachment;
            // reset interest ops 
            selectionKey.interestOps(0);
            // now always set the socket to readable state ... 
            selectionKey.interestOps(selectionKey.interestOps() | SelectionKey.OP_READ);
            
            
            if (selectionKey.isValid() && selectionKey.isReadable()) {
              int bytesRead = -1;
              
              try {
                
                timeStart = System.currentTimeMillis();
                // track the number of actual bytes read in the callback ... 
                bytesRead = listener.Readable(null);
                //System.out.println("Readable Took:" + (System.currentTimeMillis() - timeStart));
                if (timeUsageDetailOut != null) { 
                  timeUsageDetailOut.timeInReadableEvt += System.currentTimeMillis() - timeStart;
                }

                if (bytesRead == -1 ) {
                  // log it ... 
                  // LOG.error("Abnormal Disconnect Detected on Socket:"+ ((NIOClientSocket)theSocket).getSocketAddress());
                  // trigger a disconnect event ...
                  listener.Disconnected(null,null);
                }
              }
              catch (RuntimeException e) { 
                LOG.error("Caught Runtime Exception in Readable Event:"+StringUtils.stringifyException(e));
                listener.Excepted(null,e);
              }
              // if bytesRead == -1 then this means that the underlying connection has gone bad ... 
            }
            
            if (selectionKey.isValid() && selectionKey.isWritable()) { 
              try { 
                
                timeStart = System.currentTimeMillis();
                listener.Writeable(null);
                // System.out.println("Writable Took:" + (System.currentTimeMillis() - timeStart));
                if (timeUsageDetailOut != null) { 
                  timeUsageDetailOut.timeInWritableEvt += System.currentTimeMillis() - timeStart;
                }
              }
              catch (RuntimeException e) { 
                LOG.error("Caught Runtime Exception in Readable Event:"+StringUtils.stringifyException(e));
                listener.Excepted(null,e);
              }
            }
          }
        }
        else { 
          LOG.error("Invalid Socket Detected. Calling Disconnect");
          NIOSocket theSocket = (NIOSocket) selectionKey.attachment();
          if (theSocket != null && theSocket.getListener() != null) { 
            theSocket.getListener().Disconnected(theSocket,null);
          }
        }
      }
      //timeoutThriftMethods();
      //startPendingThriftMethods();      
      
      long unblockedTimeEnd = System.currentTimeMillis();
      if (timeUsageDetailOut != null) { 
        timeUsageDetailOut.unblockedTime += (unblockedTimeEnd - unblockedTimeStart);
      }
      
    // }
    return count;
  }

  /** wakeup the potentially blocked primary poll thread - used by a worker thread to unblock the primary 
   * poll thread when a non-io event has resulted in a condition that needs to be addressed in the primary 
   * thread context. 
   * 
   * @throws IOException
   */
  public void wakeup() throws IOException { 

    if (_selector == null || !_selector.isOpen()) {
    	
      IOException e = new IOException("Selector NULL or Selector is Not Open!");
      LOG.error(e);
      throw e;
    }
    
    _selector.wakeup();
  }
    
  
  /** Comparator used in TreeSet */
  /*
  private static class TAsyncMethodCallTimeoutComparator implements Comparator<TAsyncMethodCall> {
    @SuppressWarnings("unchecked")
    @Override
    public int compare(TAsyncMethodCall left, TAsyncMethodCall right) {
      if (left.getTimeoutTimestamp() == right.getTimeoutTimestamp()) {
        return (int)(left.getSequenceId() - right.getSequenceId());
      } else {
        return (int)(left.getTimeoutTimestamp() - right.getTimeoutTimestamp());
      }
    }
  }  
  
  // Timeout any existing method calls
  @SuppressWarnings("unchecked")
  private void timeoutThriftMethods() {
    Iterator<TAsyncMethodCall> iterator = timeoutWatchSet.iterator();
    long currentTime = System.currentTimeMillis();
    while (iterator.hasNext()) {
      TAsyncMethodCall methodCall = iterator.next();
      if (currentTime >= methodCall.getTimeoutTimestamp()) {
        iterator.remove();
        methodCall.onError(new TimeoutException("Operation " + methodCall.getClass() + " timed out after " + (currentTime - methodCall.getStartTime()) + " ms."));
      } else {
        break;
      }
    }
  }
  
  // Start any new calls
  @SuppressWarnings("unchecked")
  private void startPendingThriftMethods() {
    TAsyncMethodCall methodCall;
    while ((methodCall = pendingCalls.poll()) != null) {
      // Catch registration errors. method will catch transition errors and cleanup.
      try {
        methodCall.start(this._selector);

        // If timeout specified and first transition went smoothly, add to timeout watch set
        TAsyncClient client = methodCall.getClient();
        if (client.hasTimeout() && !client.hasError()) {
          timeoutWatchSet.add(methodCall);
        }
      } catch (Exception exception) {
        LOG.warn("Caught exception in TAsyncClientManager!", exception);
        methodCall.onError(exception);
      }
    }
  }
  
  // Transition methods for ready keys
  @SuppressWarnings("unchecked")
  private void transitionThriftMethod(TAsyncMethodCall method,SelectionKey key) {
    try {
      method.transition(key);

      // If done or error occurred, remove from timeout watch set
      if (method.isFinished() || method.getClient().hasError()) {
        timeoutWatchSet.remove(method);
      }
    } catch (ClosedSelectorException e) {
      LOG.error("Caught ClosedSelectorException in TAsyncClientManager!", e);
    }
  }
  */  
}
