package org.commoncrawl.query;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.commoncrawl.rpc.RPCStruct;

public interface QueryProgressCallback<DataType extends RPCStruct,ResultKeyType extends WritableComparable,ResultValueType extends Writable> { 
  /** return FALSE from updateProgress to cancel query **/
  boolean updateProgress(Query<DataType,ResultKeyType,ResultValueType> theQueryObject,float percentComplete);
}
