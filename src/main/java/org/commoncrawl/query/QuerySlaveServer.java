package org.commoncrawl.query;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataInputBuffer;
import org.commoncrawl.async.ConcurrentTask;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.async.Callbacks.Callback;
import org.commoncrawl.query.BaseConfig;
import org.commoncrawl.query.QueryCommon;
import org.commoncrawl.query.QueryServerSlave;
import org.commoncrawl.query.QueryStatus;
import org.commoncrawl.query.RemoteQueryInfo;
import org.commoncrawl.query.SlaveStatus;
import org.commoncrawl.rpc.BinaryProtocol;
import org.commoncrawl.rpc.EmptyStruct;
import org.commoncrawl.rpc.IncomingMessageContext;
import org.commoncrawl.rpc.RPCException;
import org.commoncrawl.rpc.RPCStruct;
import org.commoncrawl.rpc.MessageData.Status;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.FileUtils;


public abstract class QuerySlaveServer implements     
  QueryServerSlave, 
  QueryProgressCallback,
  RemoteQueryCompletionCallback { 
  

  static final Log LOG = LogFactory.getLog(QuerySlaveServer.class);

    
  //////////////////////////////////////////////////////////////////////////////
  //query support
  //////////////////////////////////////////////////////////////////////////////
  
  private LinkedList<Query>  _pendingQueries   = new LinkedList<Query>();
  private Map<Long,Query>    _activeQueries    = new HashMap<Long,Query>();
  private HashSet<Long>      _cancelledQueries = new HashSet<Long>(); 
  
  private boolean       _cancelling = false;
  private BaseConfig    _baseConfig;
  private SlaveStatus   _slaveStatus = new SlaveStatus();
  private String        _fqName;
  private Timer         _pollTimer;
  
  public static final int SLAVE_MAX_CONCURRENT_QUERIES_DEFAULT = 1;
  public static final String SLAVE_MAX_CONCURRENT_QUERIES_PARAM = "query.slave.max.concurrent.queries";
  
  public abstract Query     createQueryObjectGivenType(String queryObjectType,int shardId,RPCStruct queryDataObject) throws IOException;
  public abstract RPCStruct createQueryDataObjectGivenType(String queryDataObjectType);
  public abstract File      getLocalQueryTempDir();
  public abstract EventLoop getEventLoop();
  public abstract FileSystem getRemoteFileSystem();
  public abstract Configuration getConfiguration();
  public abstract ExecutorService getQueryThreadPool();
  
  

  public QuerySlaveServer(String fqName) { 
    _fqName = fqName;
  }
  
  public String getFQName() { 
    return _fqName;
  }
  
  @Override
  public void cancelQuery(
      IncomingMessageContext<QueryCommon, EmptyStruct> message)
      throws RPCException {
    if (_activeQueries.containsKey(message.getInput().getQueryId())) { 
      _cancelledQueries.add(message.getInput().getQueryId());
    }
  }

  @Override
  public void doQuery(IncomingMessageContext<RemoteQueryInfo, QueryStatus> rpcContext)throws RPCException {

    LOG.info(_fqName + " Adding Query Type:"+ rpcContext.getInput().getQueryClassType() + "Id:" + rpcContext.getInput().getCommonInfo().getQueryId() + " to Queue.");
    try {
      // extract object type 
      String queryObjectType = rpcContext.getInput().getQueryClassType();
      LOG.info(_fqName + " QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " ObjectType:" + queryObjectType);
      // and data type 
      String queryDataType   = rpcContext.getInput().getQueryDataClassType();
      LOG.info(_fqName + " QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " QueryDataType:" + queryDataType);
      // allocate the object data type .. 
      RPCStruct queryData = createQueryDataObjectGivenType(queryDataType);
      LOG.info(_fqName + " QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " DeSerializing Query Data");
      // allocate an input stream  
      DataInputBuffer inputStream = new DataInputBuffer();
      inputStream.reset(rpcContext.getInput().getQueryDataBuffer().getReadOnlyBytes(),0,rpcContext.getInput().getQueryDataBuffer().getCount());
      // and deserialize into the structure 
      queryData.deserialize(inputStream,new BinaryProtocol());
      LOG.info(_fqName + " QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " Allocating QueryOp Object of type:" + queryObjectType);
      // allocate query object 
      Query queryObject = createQueryObjectGivenType(queryObjectType,rpcContext.getInput().getShardId(),queryData);
      LOG.info(_fqName + " QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " Initializing QueryObject");
      // initialize query 
      queryObject.initializeRemoteQuery(this,rpcContext.getInput(),queryData);
      LOG.info(_fqName + " QueryId:" + rpcContext.getInput().getCommonInfo().getQueryId() + " Adding to Pending Queue");
      //TODO: SEE IF WE CAN IMMEDIATELY EXECUTE QUERY ...
      if (queryObject.isHighPriorityQuery()) { 
        // high priority query ... dispatch immediately ...
        activateQuery(queryObject);
      }
      else { 
          // add to pending set ... 
          _pendingQueries.add(queryObject);
      }
      // add query to query status structure ...
      updateSlaveStatusForQueryObject(queryObject);
      // now potentially start next query ... 
      potentiallyStartNextQuery();
      // now send the query's current status back to caller 
      rpcContext.getOutput().merge(queryObject.getQueryStatus());
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      LOG.error("Query Dispatch for Query Id:" + rpcContext.getInput().getCommonInfo().getQueryId() + " Failed with Exception:" + CCStringUtils.stringifyException(e));
      rpcContext.setStatus(Status.Error_RequestFailed);
      rpcContext.setErrorDesc(CCStringUtils.stringifyException(e));
    }
    // complete request ... 
    rpcContext.completeRequest();    
  }

  @Override
  public void heartbeatQuerySlave(
      IncomingMessageContext<EmptyStruct, SlaveStatus> message)
      throws RPCException {
    sendStatusResponse(message);    
  }
  
  private int _maxConcurrentQueries = SLAVE_MAX_CONCURRENT_QUERIES_DEFAULT;
  private static final int POLL_TIMER_DELAY = 100;
  @Override
  public void initializeQuerySlave(final IncomingMessageContext<BaseConfig, SlaveStatus> messageContext)throws RPCException {
    
    _maxConcurrentQueries = getConfiguration().getInt(SLAVE_MAX_CONCURRENT_QUERIES_PARAM, SLAVE_MAX_CONCURRENT_QUERIES_DEFAULT);
    
    // terminate all active queries ... 
    terminateAndFlushAllQueries(
        
        new Callback() {

          @Override
          public void execute() {

            // we are still in the async thread here ... all existing queries have been cancelled at this point ... 
            // clear query info 
            _activeQueries.clear();
            _pendingQueries.clear();
            
            // clear out state ... 
            _slaveStatus.clear();
            _slaveStatus.setState(SlaveStatus.State.INITIALIZING);
            
            // reset cancel flag 
            _cancelling = false;
            
            // set up base config ... 
            try {
              _baseConfig = (BaseConfig) messageContext.getInput().clone();
            } catch (CloneNotSupportedException e) {
            }

            LOG.info(_fqName + " All Data Files successfully loaded. finishing initialization");
            finishInitialize(messageContext);
          }
          
        });
    
    _pollTimer = new Timer(POLL_TIMER_DELAY,true, new Timer.Callback() {
      
      @Override
      public void timerFired(Timer timer) {
        potentiallyStartNextQuery();
      }
    });
    getEventLoop().setTimer(_pollTimer);
  }
  

  
  @Override
  public boolean updateProgress(Query theQueryObject, float percentComplete) {
    LOG.info(_fqName + " Update Progress Received for Query:" + theQueryObject.getQueryId() + "pctComplete:" + percentComplete);


    if (!_cancelling) { 
      synchronized (_cancelledQueries) {
        // if the query object is in the cancelled set ... 
        if (_cancelledQueries.contains(theQueryObject.getQueryId())) {
          // return false to indicate that query execution should terminate prematurely 
          return false;
        }
      }
      // update query status in slave's data structure  
      updateSlaveStatusForQueryObject(theQueryObject);
      // return true to indicate that query execution should continue 
      return true;
    }
    else { 
      // return false to indicate that query execution should terminate prematurely 
      return false;
    }
  }  
  
  @Override
  public void queryComplete(Query request, long resultCount) {
    LOG.info(_fqName + " QueyComplete received for Query:" + request.getQueryId() + " resultCount:" + resultCount);
    // this callback occurs in the context of the async thread ...
    if (!_cancelling) {
      synchronized (_cancelledQueries) { 
        // if this query was cancelled ... 
        if (_cancelledQueries.contains(request.getQueryId())) {
          // clear out the entry in the array 
          _cancelledQueries.remove(request.getQueryId());
          LOG.info(_fqName + " Query Seems to have been cancelled. Explicitly cancelling Query:" + request.getQueryId());
          // override status 
          request.getQueryStatus().setStatus(QueryStatus.Status.CANCELLED);
        }
      }
      // update the slave status according to the query status
      updateSlaveStatusForQueryObject(request);
      // remove the query from the active queue ... 
      _activeQueries.remove(request.getQueryId());
      
      FileUtils.recursivelyDeleteFile(getTempDirForShardedQuery(request));
    }
  }

  @Override
  public void queryFailed(Query request, String reason) {
    LOG.info(_fqName + " QueryFailed received for Query:" + request.getQueryId() + " reason:" + reason);
    if (!_cancelling) {
     
     synchronized (_cancelledQueries) { 
       // if this query was cancelled ... 
       if (_cancelledQueries.contains(request.getQueryId())) {
         // clear out the entry in the array 
         _cancelledQueries.remove(request.getQueryId());
         // override status 
         request.getQueryStatus().setStatus(QueryStatus.Status.CANCELLED);
       }
     }
     // update the slave status according to the query status
     updateSlaveStatusForQueryObject(request);
     // remove the query from the active queue ... 
     _activeQueries.remove(request.getQueryId());
     
     FileUtils.recursivelyDeleteFile(getTempDirForShardedQuery(request));
   }
    
  }  
  
  ////////////////////////////////////////////////////////////////////////////
  // Query Support 
  ////////////////////////////////////////////////////////////////////////////
  
  private File getTempDirForShardedQuery(Query query) {
    return new File(getLocalQueryTempDir(),Long.toString(query.getQueryId()) +"-"+query.getRemoteQueryInfo().getShardId());
  }
  
  private void activateQuery(Query queryObject) { 
    LOG.info(_fqName + " Activating Query:" + queryObject.getQueryId());
    _activeQueries.put(queryObject.getQueryId(),queryObject);

    try {
      LOG.info(_fqName + " Starting Slave Query for Query:" + queryObject.getQueryId());
      // start the query thread ...
      queryObject.runShardedQuery(_fqName,getRemoteFileSystem(),getConfiguration(),getEventLoop(),this,getTempDirForShardedQuery(queryObject),this,this);
      // and update the status.
      updateSlaveStatusForQueryObject(queryObject);
    } catch (IOException e) {
      LOG.info(_fqName + " Query Activation for Query:"+ queryObject.getQueryId() +" Failed with Exception:" + CCStringUtils.stringifyException(e));
      // remove from active list ... 
      _activeQueries.remove(queryObject.getQueryId());
      synchronized (queryObject) {
        // mark as failed ... 
        queryObject.getQueryStatus().setStatus(QueryStatus.Status.ERROR);
        queryObject.getQueryStatus().setOptErrorReason(CCStringUtils.stringifyException(e));
      }
      FileUtils.recursivelyDeleteFile(getTempDirForShardedQuery(queryObject));

      updateSlaveStatusForQueryObject(queryObject);
    }
    
  }
  
  private void finishInitialize(IncomingMessageContext<BaseConfig, SlaveStatus> rpcContext) { 
    // and update slave status state 
    _slaveStatus.setState(SlaveStatus.State.READY);
    sendStatusResponse(rpcContext);
  }

  private void sendStatusResponse(IncomingMessageContext<? extends RPCStruct,SlaveStatus> context) { 
    try {
      // get base status
      context.setOutput((SlaveStatus) _slaveStatus.clone());
      
      // log it ... 
      if (context.getOutput().getQueryStatus().size() != 0) { 
        //LOG.info(_fqName + " Sending a non-zero query status list in heartbeat response");
      }
      // clear query status in slave status ... 
      _slaveStatus.getQueryStatus().clear();
      
    } catch (CloneNotSupportedException e) {
    }
    try {
      context.completeRequest();
    } catch (RPCException e) {
      LOG.error("failed to send StatusResponse to incoming RPC!");
    }
  }

  private void terminateAndFlushAllQueries(final Callback callback) { 
    
    _cancelling = true;
    
    if (_activeQueries.size() == 0) {
      // execute callback immediately 
      callback.execute();
    }
    else { 
      // otherwise terminate queries in a background thread ... 
      final Vector<Query> activeQueries = new Vector<Query>(_activeQueries.values());
          
      getQueryThreadPool().submit(new ConcurrentTask<Boolean>(getEventLoop(),new Callable<Boolean>() {
  
        
        @Override
        public Boolean call() throws Exception {
          LOG.info(_fqName + " Starting Cancel Thread");
           for (Query query : activeQueries) {
             LOG.info(_fqName + " Cancelling Query:" + query.getQueryId());
             try { 
               query.cancelSlaveQuery();
             }
             catch (Exception e) { 
               LOG.error("Error Cancelling Query:" + query.getQueryId() + " Error:" + CCStringUtils.stringifyException(e));
             }
             LOG.info(_fqName + " Cancelled Query:" + query.getQueryId());
           }
           
           return true;
        } 
        
      }, new ConcurrentTask.CompletionCallback<Boolean>() {
  
        @Override
        public void taskComplete(Boolean loadResult) {
          _cancelling = false;
          callback.execute();
        }
  
        @Override
        public void taskFailed(Exception e) {
          _cancelling = false;
          LOG.error(CCStringUtils.stringifyException(e));
          callback.execute();
        } 
        
      }));
    }
  }

  private void updateSlaveStatusForQueryObject(Query theQueryObject) { 
    //LOG.info(_fqName + " Updating Query Status for Query:" + theQueryObject.getQueryId() + " Status:" + QueryStatus.Status.toString(theQueryObject.getQueryStatus().getStatus()));
    
    long queryId = -1;
    int  shardId = -1;
    synchronized (theQueryObject) {
      queryId = theQueryObject.getQueryId();
      shardId = theQueryObject.getRemoteQueryInfo().getShardId();
    }
    
    QueryStatus targetStatus = null;
    
    synchronized (_slaveStatus) {
      for (QueryStatus status : _slaveStatus.getQueryStatus()) { 
        // if query ids and shard ids match 
        if (status.getQueryId() == queryId 
              && status.getShardId() == shardId) { 
          targetStatus = status;
          break;
        }
      }
    }
    
    boolean addNewStatus = (targetStatus == null);
    if (targetStatus == null) { 
      targetStatus = new QueryStatus();
    }
    synchronized (theQueryObject) {
      // clear fields that are only relavent at the master 
      theQueryObject.getQueryStatus().setFieldClean(QueryStatus.Field_ATTEMPTS);
      theQueryObject.getQueryStatus().setFieldClean(QueryStatus.Field_LASTATTEMPTTIME);
      
      try {
        targetStatus.merge(theQueryObject.getQueryStatus());
      } catch (CloneNotSupportedException e) {
      }
    }
    
    if (addNewStatus) { 
      synchronized (_slaveStatus) {
        _slaveStatus.getQueryStatus().add(targetStatus);
      }
    }
  }

  private void potentiallyStartNextQuery() {
    
    while (_activeQueries.size() < _maxConcurrentQueries && _pendingQueries.size() != 0) { 
      // remove next from queue 
        Query queryObject = _pendingQueries.removeFirst();
      // and activate 
        activateQuery(queryObject);
    }
  }

  
}
