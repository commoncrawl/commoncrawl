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

package org.commoncrawl.async;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A FutureTask that runs concurrently but returns status via an async callback
 * in the specified event loop
 * 
 * @author rana
 *
 * @param <V> Value Type
 */
public class ConcurrentTask<V>  extends FutureTask<V>{

  /** logging **/
  private static final Log LOG = LogFactory.getLog(ConcurrentTask.class);

  private EventLoop _eventLoop;
  private CompletionCallback<V> _callback;
  
  public static interface CompletionCallback<V> { 
    
    void taskComplete(V loadResult);
    void taskFailed(Exception e);
  }
  
  public ConcurrentTask(EventLoop eventLoop,Callable<V> callable,CompletionCallback<V> callback) {
    super(callable);

    _eventLoop = eventLoop;
    _callback     = callback;
  }

  protected void done() {

    // schedule an async event to process loaded results ...
    AsyncTask.runAsyncTask(_eventLoop,new Runnable() {

      @Override
      public void run() {
        try {
          _callback.taskComplete(get());
        } catch (InterruptedException e) {
          LOG.error(e);
        } catch (ExecutionException e) {
          LOG.error(e);
          _callback.taskFailed(e);
        }
        catch (CancellationException e) { 
          LOG.error(e);
          _callback.taskFailed(e);
        }
      } 
      
    },false);
  }
}
