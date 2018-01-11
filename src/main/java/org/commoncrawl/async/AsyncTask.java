package org.commoncrawl.async;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.util.shared.CCStringUtils;

/**
 * A task that executes in the specified event loop and optinally blocks calling thread 
 * until task is completed.
 *  
 * @author rana
 *
 */
public class AsyncTask {
  
  /** logging **/
  private static final Log LOG = LogFactory.getLog(AsyncTask.class);
    
  /**
   * 
   * @param eventLoop
   * @param task
   * @param waitForResult
   */
  public static void runAsyncTask(final EventLoop eventLoop,final Runnable task,final boolean waitForResult) {

    if (eventLoop.getEventThread() == Thread.currentThread()) {
      try {
        task.run();
      } catch (Exception e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
    else { 
      final Semaphore blockingSemaphore = new Semaphore(0);
      
      // schedule an async event to process loaded results ... 
      Timer timer = new Timer(0,false,new Timer.Callback() {

        public void timerFired(Timer timer) {
          try {
            task.run();
          } catch (Exception e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
          finally {
            blockingSemaphore.release();
          }
        } 
      });
        // schedule the timer in the server's main event loop thread ... 
        eventLoop.setTimer(timer);
      
      try {
        if (waitForResult)
          blockingSemaphore.acquire();
      } catch (InterruptedException e) {
      }
    }
  }
  
  static class ResultContainer<ResultType> { 
    private ResultType result = null;
    
    ResultContainer() { 
      
    }
    
    void setResult(ResultType result) {
      this.result = result;
    }
    
    ResultType getResult() { 
      return result;
    }
  }
    
  /**
   * 
   * @param <ResultType> result type 
   * @param eventLoop event loop to execute task in 
   * @param callable the callable object
   * @return
   */
  public static <ResultType> ResultType runAsyncTaskWithResult(final EventLoop eventLoop,final Callable<ResultType> callable) {

    if (eventLoop.getEventThread() == Thread.currentThread()) {
      try {
        return callable.call();
      } catch (Exception e) {
        LOG.error(CCStringUtils.stringifyException(e));
        return null;
      }
    }
    else { 
      
      final Semaphore blockingSemaphore = new Semaphore(0);
      final ResultContainer<ResultType> resultContainer = new ResultContainer<ResultType>();
      
      // schedule an async event to process loaded results ... 
      Timer timer = new Timer(0,false,new Timer.Callback() {

        public void timerFired(Timer timer) {
          try {
            resultContainer.setResult(callable.call());
          } catch (Exception e) {
            LOG.error(CCStringUtils.stringifyException(e));
          }
          finally {
            blockingSemaphore.release();
          }
        } 
      });
      // schedule the timer in the server's main event loop thread ... 
      eventLoop.setTimer(timer);
      
      try {
        blockingSemaphore.acquire();
      } catch (InterruptedException e) {
      }
      
      return resultContainer.getResult();
    }
  }  
}
