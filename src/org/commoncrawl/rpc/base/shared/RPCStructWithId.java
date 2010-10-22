package org.commoncrawl.rpc.base.shared;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class RPCStructWithId extends RPCStruct {

  private long _recordId;
  
  
  /** get set the record id **/
  public long getRecordId() { return _recordId; }
  public void setRecordId(long recordId) { _recordId = recordId; }
  
  /** clear the structure's contents **/
  public void clear() { 
    _recordId = 0;
  }
  
}
