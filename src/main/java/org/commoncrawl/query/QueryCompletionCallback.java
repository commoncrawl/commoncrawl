package org.commoncrawl.query;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.commoncrawl.rpc.RPCStruct;


public interface QueryCompletionCallback<DataType extends RPCStruct,KeyType extends WritableComparable,ValueType extends Writable> {
    void queryComplete(QueryRequest<DataType,KeyType,ValueType> request,QueryResult<KeyType,ValueType> queryResult);
    void queryFailed(QueryRequest<DataType,KeyType,ValueType> request,String reason);
}
