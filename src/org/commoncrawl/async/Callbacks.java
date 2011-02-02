package org.commoncrawl.async;

public class Callbacks {

  /** generic callback interface **/
  public interface Callback {
    public void execute();
  }
  
  public interface CallbackWithResult<ResultType> {
    public void execute(ResultType result);
  }

}
