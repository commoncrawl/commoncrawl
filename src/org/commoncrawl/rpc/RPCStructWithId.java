package org.commoncrawl.rpc;

public abstract class RPCStructWithId<ConcreteType> extends RPCStruct<ConcreteType> {

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
