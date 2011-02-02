package org.commoncrawl.rpc.base.shared;

public abstract class RPCStructWithId extends RPCStruct {

  private long _recordId;

  /** get set the record id **/
  public long getRecordId() {
    return _recordId;
  }

  public void setRecordId(long recordId) {
    _recordId = recordId;
  }

  /** clear the structure's contents **/
  public void clear() {
    _recordId = 0;
  }

}
