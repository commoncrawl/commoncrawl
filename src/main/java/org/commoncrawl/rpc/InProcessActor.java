package org.commoncrawl.rpc;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.rpc.MessageData.Status;
import org.commoncrawl.util.shared.CCStringUtils;

/**
 * Actor - Actors are concurrent tasks that respond to messages via a strictly 
 * defined interface. An Actor can process messages in it's own EventLoop (thread) 
 * or it can utilize a passed in ThreadPool object to execute each message in the 
 * context of a worker thread. 
 * 
 * Actor is an abstract class and inorder to create an Actor instance you must
 * overload the Actor's dispatch method which is called whenever an Actor 
 * receives a message.
 * 
 * To communicate with an Actor, you open a Channel to it (via the createChannel 
 * API). You can specify a source Actor, a source EventLoop or a source ThreadPool
 * when opening a Channel to an Actor. The Actor will then send reply messages
 * to the Channel owner in the context of the sender's EventLoop thread or ThreadPool.
 * 
 * The CommonCrawl RPC Compiler allows you to specify a well defined Message 
 * Interface via the Service keyword. Whenever the compiler processes a Service 
 * definition, it automatically creates the appropriate stubs necessary to create
 * an Actor from the Service Interface specification. 
 * 
 * For example, if you defined a data structures and a serivce using the CommonCrawl
 * RPC IDL: 
 * 
 * class KeyValueTuple { 
 *  ustring key = 1;
 *  buffer  value = 2;
 * }
 * 
 * class KeyValueIOResult { 
 *  enum Status { 
 *     SUCCESS = 0;
 *     FAILURE = 1;
 *  }
 *  ustring key = 1;
 *  int     result = 2;
 *  ustring optionalErrorDesc = 3;
 * }    
 * 
 * Service KeyValueWriterService {
 *   method writeKeyValuePair(in KeyValueTuple,out KeyValueIOResult);
 * }
 *
 * The RPC compiler would create for you the following code: 
 *
 * A Service interface definition as such: 
 * 
  public interface KeyValueWriterService extends Service {
  
     void writeKeyValuePair(IncomingMessage<KeyValueTuple,KeyValueIOResult> message) throws RPCException;
  }

 * A typed stub, that in combination with a channel, you can use to make 
 * asynchronous calls to the Service: 
 * 
 * public static class AsyncStub extends Service.AsyncStub {
    
    // constructor
    public AsyncStub(Channel channel) {
      super(channel);
    }
    
    public OutgoingMessage<KeyValueTuple,KeyValueIOResult> writeKeyValuePair(KeyValueTuple input,OutgoingMessage.Callback<KeyValueTuple,KeyValueIOResult> callback) throws RPCException ...     
  }
  

  * And a Factory to construct an Actor that adheres to an responds to the 
  * Service specification: 
 
  public static class ActorFactory {
    public static Actor createActorForService(final KeyValueWriterService instance,ThreadPoolExecutor executor,Actor.Events optionalListener) throws IOException  .... 
  }
  
  ** Using this generated code, you would construct an Actor as such: 
  
  Actor keyValueWriter = KeyValueWriterService.ActorFactory.createActorForService( 
    new KeyValueWriterService() {
      
      void writeKeyValuePair(IncomingMessage<KeyValueTuple,KeyValueIOResult> message) throws RPCException { 
        
        // THIS CODE RUNS IN THE CONTEXT OF THE ACTOR's EVENTLOOP THREAD 
        // OR IN A THREAD-POOL THREAD, DEPENDING ON HOW YOU CREATED THE ACTOR
        KeyValueTuple tuple = message.getInput();
        
        ... do some work here... 
        
        message.getOutput().setKey(message.getInput().getKey());
        message.getOutput().setStatus(KeyValueIOResult.Status.SUCCESS);
        
        // WHEN YOU CALL COMPLETE REQUEST, 
        // THE MESSAGE WILL BE QUEUED UP AND EXECUTED IN THE CONTEXT OF 
        // THE CALLING THREAD'S (Channel Owners') CONTEXT.
        
        message.completeRequest(); 
      }
    },
    //no thread pool .. use event loop thread
    null,
    // event callback
    new Actor.Events() { ... }
   );
   
   .... 
   // SOME OTHER THREAD CONTEXT ... 
   // CREATE A CHANNEL TO THE ABOVE ACTOR (RESPONSES PROCESSED IN THREAD POOL)
   Channel channel = keyValueWriter.createChannel(threadPool);
   // CREATE A STUB TO TALK TO SEND MESSAGES TO THE ACTOR
   KeyValueWriterService.AsyncStub stub = new KeyValueWriterService.AsyncStub(channel);
   // create and populate a tuple ... 
   KeyValueTuple tuple = new KeyValueTuple();
   tuple.setKey ....
   // send a message to the actor via the stub  
   stub.writeKeyValuePair(tuple,new new OutgoingMessage.Callback() { 
     requestComplete(OutgoingMessage message) { 
       // this code will run in the context of the sender's thread (thread pool)
       // in this case ... 
       if (message.getStatus() == Status.Success) { 
         // do something ... 
       }
     }
   });
 * 
 * 
 *    
 * @author rana
 *
 */
public abstract class InProcessActor {
  
  public static final Log LOG = LogFactory.getLog(InProcessActor.class); /** interface specification **/

  // the event loop in which this actor resides  
  EventLoop _eventLoop;
  // owns event loop 
  boolean   _ownsEventLoop;
  // optional thread pool executor 
  ThreadPoolExecutor _executor;
  
  /**
   *  Actors broadcast startup and shutdown events
   *  If you want to do some setup work within the context of 
   *  the Actor thread (EventLoop thread), you should do so in the 
   *  actor's onStartup event callback. 
   *
   */
  public static interface Events { 
    /**
     * called from within the context of the actor thread
     * during startup
     */
    public void onStartup(InProcessActor actor);
    
    /**
     * called from within the context of the actor thread 
     * during shutdown 
     */
    public void onShutdown(InProcessActor actor);    
  }
  
  // optional event listener 
  Events _eventListener;
  
  /**
   * constructor ... Actors are constructed via typed factory methods
   * 
   * @param optionalExecutor
   * @throws IOException
   */
  public InProcessActor(ThreadPoolExecutor optionalExecutor,Events optionalEventListener) throws IOException {
    if (optionalExecutor == null) { 
      _eventLoop = new EventLoop();
      _ownsEventLoop = true;
    }
    else { 
      _executor = optionalExecutor;
    }
    _eventListener = optionalEventListener;
  }
  
  public void start() { 
    if (_ownsEventLoop) { 
      _eventLoop.start();
    }
    if (_eventListener != null) { 
      Runnable callback = new Runnable() {
  
        @Override
        public void run() {
          _eventListener.onStartup(InProcessActor.this);
        } 
        
      };
      if (_eventLoop != null && _eventLoop.getEventThread() == Thread.currentThread()) {
        callback.run();
      }
      else {
        if (_eventLoop != null)
          _eventLoop.queueAsyncRunnable(callback);
        else
          _executor.submit(callback);
      }
    }
  }
  
  public void stop() {
    
    Runnable callback = new Runnable() {

      @Override
      public void run() {
        if (_eventListener != null) { 
          _eventListener.onShutdown(InProcessActor.this);
        }
        
        if (_ownsEventLoop) { 
          _eventLoop.stop();
        }
      }
    };
    if (_eventLoop != null && _eventLoop.getEventThread() == Thread.currentThread()) { 
      callback.run();
    }
    else { 
      if (_eventLoop != null)
        _eventLoop.queueAsyncRunnable(callback);
      else 
        _executor.submit(callback);
    }
  }
  
  @SuppressWarnings("unchecked")
  public static class IncomingMessage extends IncomingMessageContext {
    OutgoingMessageContext _source;
    public IncomingMessage(OutgoingMessageContext source,Channel channel,int requestId, RPCStruct input, RPCStruct output) {
      super(channel, requestId, input, output);
      _source = source;
    } 
    
    public String getServiceName() { 
      return _source._serviceName;
    }
    
    public String getMethodName() { 
      return _source._methodName;
    }
  }
  
  /**
   * create a channel to This actor and send responses to the 
   * specified source Actor
   * 
   * @param sourceActor
   * @return a Channel object
   */
  public final Channel createChannel(final InProcessActor sourceActor) {
    return createChannel(sourceActor,null,null);
  }
  
  /**
   * create a channel to This actor and run the response callback  
   * in the specified EventLoop 
   * @param sourceEventLoop
   * @return a Channel object
   */
  public final Channel createChannel(final EventLoop sourceEventLoop) { 
    return createChannel(null,sourceEventLoop,null);
  }

  /**
   * create a channel to This actor and run the response callback
   * in the specified ThreadPool
   * 
   * @param sourceExecutor
   * @return a Channel object
   */
  public final Channel createChannel(ThreadPoolExecutor sourceExecutor) { 
    return createChannel(null,null,sourceExecutor);
  }

  /**
   * create a channel to This actor and run the response callback
   * in target actor's EventLoop or ThreadPool
   * 
   * @return a Channel object
   */
  
  public final Channel createChannel() {
    return createChannel(null,null,null);
  }

  /**
   * An extension of Channel that adds awareness of the attached InProcessActor 
   * @author rana
   *
   */
  public static interface InProcessChannel extends Channel { 
    public InProcessActor getActor();
  }
  
  
  private final Channel createChannel(final InProcessActor optionalSourceActor,final EventLoop optionalSourceEventLoop,final ThreadPoolExecutor optionalSourceExecutor) { 
    
    return new InProcessChannel() {

      InProcessActor     _optionalSourceActor = optionalSourceActor;
      EventLoop _optionalSourceEventLoop = optionalSourceEventLoop;
      ThreadPoolExecutor _optionalSourceExecutor = optionalSourceExecutor;

      @Override
      public void sendRequest(final OutgoingMessageContext<? extends RPCStruct, ? extends RPCStruct> originalMessage)throws RPCException {
        final IncomingMessage incomingMessage 
          = new IncomingMessage(originalMessage, 
            this, 
            originalMessage.getRequestId(), 
            (RPCStruct)originalMessage.getInput(),(RPCStruct)originalMessage.getOutput());

        final Channel channel = this;
        
        Runnable callback = new Runnable() {

          @SuppressWarnings("unchecked")
          @Override
          public void run() {
            try {
              dispatch(channel, incomingMessage);
            }
            catch (RPCException e) { 
              LOG.error(CCStringUtils.stringifyException(e));
              incomingMessage.setStatus(Status.Error_RPCFailed);
              incomingMessage.setErrorDesc(CCStringUtils.stringifyException(e));
              try {
                sendResponse(incomingMessage);
              } catch (RPCException e1) {
                LOG.error(CCStringUtils.stringifyException(e1));
              }
            }
          } 
          
        };
        
        if (_executor != null) { 
         _executor.submit(callback); 
        }
        else {
          _eventLoop.queueAsyncRunnable(callback);
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public void sendResponse(final IncomingMessageContext<? extends RPCStruct, ? extends RPCStruct> message)
          throws RPCException {
        
        Runnable closure = new Runnable() {

          @Override
          public void run() {
              final IncomingMessage localMessage = (IncomingMessage)message;
              
              if (localMessage._source.getCallback() != null) { 
                localMessage._source.setOutput(message.getOutput());
                localMessage._source.setStatus(message.getStatus());
                localMessage._source.setErrorDesc(message.getErrorDesc());
                
                localMessage._source.getCallback().requestComplete(localMessage._source);
              }
            }
        };
         
        if (_optionalSourceActor != null) { 
          if (_optionalSourceActor._executor != null) {
            _optionalSourceActor._executor.submit(closure);
          }
          else  {
            _optionalSourceActor._eventLoop.queueAsyncRunnable(closure);
          }
        }
        else if (_optionalSourceEventLoop != null){
          _optionalSourceEventLoop.queueAsyncRunnable(closure);
        }
        else if (_optionalSourceExecutor != null) { 
          _optionalSourceExecutor.submit(closure);
        }
        else { 
          closure.run();
        }
      }
      
      @Override
      public InProcessActor getActor() {
        return InProcessActor.this;
      }
    };
  }
  

  
  
  /** 
   * Overload this method to handle message dispatches ...  
   * 
   * @param channel
   * @param message
   * @throws RPCException
   */
  abstract public void dispatch(final Channel channel, final IncomingMessage message) throws RPCException; 
  
}
