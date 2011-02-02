package org.commoncrawl.thriftrpc;

import org.apache.thrift.TBase;
import org.apache.thrift.async.AsyncMethodCallback;

@SuppressWarnings("unchecked")
public class ThriftAsyncRequest<InputArgs extends TBase,OutputArgs extends TBase> {

  public String methodName_;
  public InputArgs args_;
  public OutputArgs result_;
  public ThriftAsyncClientChannel channel_;
  public ThriftAsyncRequestCallback<InputArgs,OutputArgs> resultHandler_;
  public int requestId_;
  
  
  public static interface ThriftAsyncRequestCallback<InputArgs extends TBase,OutputArgs extends TBase> { 
    /**
     * This method will be called when the remote side has completed invoking
     * your method call and the result is fully read. For oneway method calls,
     * this method will be called as soon as we have completed writing out the
     * request.
     * @param response
     */
    public void onComplete(ThriftAsyncRequest<InputArgs,OutputArgs> request);

    /**
     * This method will be called when there is an unexpected clientside
     * exception. This does not include application-defined exceptions that
     * appear in the IDL, but rather things like IOExceptions.
     * @param exception
     */
    public void onError(ThriftAsyncRequest<InputArgs,OutputArgs> request,Exception exception);
    
  }
  
  public ThriftAsyncRequest(ThriftAsyncClientChannel channel,
      ThriftAsyncRequestCallback<InputArgs,OutputArgs> resultHandler,final String methodName,InputArgs args,
      OutputArgs result) {
    
    methodName_ = methodName;
    args_ = args;
    result_ = result;
    channel_ = channel;
    resultHandler_ = resultHandler;
    requestId_ = -1;
  }
  
  public void setRequestId(int requestId) { 
    requestId_ = requestId;
  }
  
  public int getRequestId() { 
    return requestId_;
  }
  
  public ThriftAsyncRequestCallback<InputArgs,OutputArgs> getCallback() {
    return resultHandler_;
  }

  public String getMethodName() { 
    return methodName_;
  }
  
  public InputArgs getInputArgs() { 
    return args_;
  }
  
  public OutputArgs getResultArgs() { 
    return result_;
  }
}
