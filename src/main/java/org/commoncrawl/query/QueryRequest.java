package org.commoncrawl.query;

import java.io.File;
import java.security.acl.LastOwnerException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.commoncrawl.query.ClientQueryInfo;
import org.commoncrawl.query.QueryStatus;
import org.commoncrawl.rpc.RPCStruct;


public class QueryRequest<DataType extends RPCStruct,KeyType extends WritableComparable,ValueType extends Writable> implements Comparable<QueryRequest>{ 

  private static final Log LOG = LogFactory.getLog(QueryRequest.class);
  private static AtomicLong _lastSeqNo = new AtomicLong(); 
  private long _querySeqNo = _lastSeqNo.addAndGet(1);
  private Query<DataType,KeyType,ValueType>            _querySource;
  private ClientQueryInfo _clientQueryObj;
  private QueryStatus _queryStatus;
  private QueryCompletionCallback<DataType,KeyType,ValueType> _completionCallback;
  
  public enum RunState { 
    IDLE,
    RUNNING_REMOTE,
    RUNNING_LOCAL,
    RUNNING_CACHE,
    ERROR
  }
  
  private RunState _runState 		 = RunState.IDLE;
  private RunState _lastRunningState = RunState.IDLE;
  
  public QueryRequest(Query<DataType,KeyType,ValueType> query,ClientQueryInfo ClientQueryInfo,QueryCompletionCallback<DataType,KeyType,ValueType> callback) { 
    _querySource = query;
    _clientQueryObj = ClientQueryInfo;
    _completionCallback = callback;
    _queryStatus = new QueryStatus();
    _queryStatus.setQueryId(ClientQueryInfo.getClientQueryId());
    _queryStatus.setStatus(QueryStatus.Status.PENDING);
  }
  
  public Query<DataType,KeyType,ValueType>           getSourceQuery() { return _querySource; }
  public ClientQueryInfo getClientQueryInfo() { return _clientQueryObj; }
  public QueryStatus getQueryStatus() { return _queryStatus; }
  public QueryCompletionCallback<DataType,KeyType,ValueType> getCompletionCallback() { return _completionCallback; }
  public void setRunState(RunState runState) {
    if (_runState != RunState.IDLE && _runState != RunState.ERROR) { 
      _lastRunningState = _runState;
    }
  	_runState = runState;
  }
  public RunState getRunState() { return _runState; }
  public RunState getLastRunState() { return _lastRunningState; }
  
  @Override
  public int compareTo(QueryRequest o) {
    return (_querySeqNo < o._querySeqNo) ? -1 : (_querySeqNo > o._querySeqNo) ? 1 : 0;
  }
  
  public long getSequenceNo() { return _querySeqNo; }
  
  
}
