package org.commoncrawl.query;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.async.Callbacks.Callback;
import org.commoncrawl.query.BaseConfig;
import org.commoncrawl.query.ClientQueryInfo;
import org.commoncrawl.query.QueryInputSplit;
import org.commoncrawl.query.PersistentQueryInfo;
import org.commoncrawl.query.QueryStatus;
import org.commoncrawl.query.SlaveStatus;
import org.commoncrawl.query.QueryRequest.RunState;
import org.commoncrawl.rpc.RPCStruct;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.FileUtils;
import org.commoncrawl.util.shared.Tuples.Pair;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.gson.stream.JsonWriter;

public class QueryController {

  QueryControllerHost _host;
  FileSystem          _remoteFS;
  Configuration       _conf;
  Path                _hdfsResultsDir;
  Path                _hdfsQueryHistoryDir;
  private PriorityQueue<QueryRequest> _queuedClientQueries = new PriorityQueue<QueryRequest>();
  @SuppressWarnings("unchecked")
  private Map<Long,QueryRequest>  _activeRemoteOrLocalQueries    = new HashMap<Long,QueryRequest>();
  @SuppressWarnings("unchecked")
  private Set<QueryRequest>       _activeRemoteQueries          = new HashSet<QueryRequest>();
  private Set<QueryRequest>       _activeLocalQueries          = new HashSet<QueryRequest>();
  private File       _localTempDir;
  private int        _maxConcurrentQueries;
  
  
  private static final Log LOG = LogFactory.getLog(QueryController.class);  

  /**
   * 
   * @param host
   * @param remoteFS
   * @param conf
   * @param hdfsResultsDir
   * @param hdfsQueryHistoryDir
   * @param localTempDir
   * @param maxConcurrentQueries
   */
  public QueryController(
      QueryControllerHost host,
      FileSystem remoteFS,
      Configuration conf,
      Path hdfsResultsDir,
      Path hdfsQueryHistoryDir,
      File localTempDir,
      int maxConcurrentQueries) { 
    
    _host = host;
    _remoteFS = remoteFS;
    _conf = conf;
    _hdfsResultsDir = hdfsResultsDir;
    _hdfsQueryHistoryDir = hdfsQueryHistoryDir;
    _localTempDir = localTempDir;
    _maxConcurrentQueries = maxConcurrentQueries;
  }
  
  /**
   * 
   * @return the controlling host ... 
   */
  
  public QueryControllerHost getHost() { 
    return _host;
  }

  
  
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // INTERNAL ROUTINES 
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  @SuppressWarnings("unchecked")
  void slaveStatusChanged(String fqnHostName,SlaveStatus slaveStatus) {
    //LOG.info("Received slaveStatusChanged from slave:" + fqnHostName);
    
    if (slaveStatus != null && slaveStatus.getQueryStatus().size() != 0) { 
      
      //LOG.info("Received:" + slaveStatus.getQueryStatus() + " QueryStatus updated from Slave:" +  fqnHostName);

      // broadcast all query changes ... 
      for (QueryStatus queryStatus : slaveStatus.getQueryStatus()) { 
        //LOG.info("RCVD Status for Query:" + queryStatus.getQueryId() + " Status:" + QueryStatus.Status.toString(queryStatus.getStatus()));
        QueryRequest request = _activeRemoteOrLocalQueries.get(queryStatus.getQueryId());
        if (request != null) { 
          //LOG.info("FOUND QueryRequestObj:" + request + " for Query:" + queryStatus.getQueryId());
          try {
            request.getSourceQuery().updateQueryStatus(queryStatus);
          } catch (IOException e) {
            LOG.error("Error Updating QueryStatus for Query:" 
                + request.getSourceQuery().getQueryId() 
                + " Slave:" + fqnHostName 
                + " Error:" + CCStringUtils.stringifyException(e));
          }
        }
        else { 
          LOG.error("DID NOT FIND QueryRequestObj for Query:" + queryStatus.getQueryId());
        }
      }
      // clear query status array ...
      slaveStatus.getQueryStatus().clear();
    }
          
    try { 
      potentiallyStartNextQuery();
    }
    catch (IOException e) { 
      LOG.error("Error encountered calling startNextQuery. Exception:" + CCStringUtils.stringifyException(e));
    }
  }

  
  public BaseConfig getBaseConfigForSlave(String fqnHostName) { 
    
    BaseConfig baseConfig = new BaseConfig();
    
    //baseConfig.setBaseWorkingDir(_hdfsWorkingDir.toString());
    baseConfig.setQueryResultsDir(_hdfsResultsDir.toString());
    baseConfig.setSlaveName(fqnHostName);
    //baseConfig.setQueryCacheDir(_hdfsResultsCacheDir.toString());
    
    return baseConfig;
  }

  public static class BlockingQueryResult<KeyType,ValueType> {
    
    public BlockingQueryResult(QueryResult<KeyType,ValueType> resultObject) { 
      querySucceeded = true;
      this.resultObject = resultObject;
    }

    public BlockingQueryResult(String failureReason) { 
      querySucceeded = false;
      this.errorString = failureReason;
    }
    
    public boolean querySucceeded = false;
    public QueryResult<KeyType,ValueType> resultObject;
    public String errorString;
  }
  
  @SuppressWarnings("unchecked")
  public <DataType extends RPCStruct,KeyType extends WritableComparable,ValueType extends Writable> BlockingQueryResult<KeyType,ValueType> blockingQueryRequest(final Query<DataType,KeyType,ValueType> queryObject,final ClientQueryInfo queryInfo) throws IOException { 
    
    final LinkedBlockingQueue<BlockingQueryResult<KeyType,ValueType>> queue = new LinkedBlockingQueue<BlockingQueryResult<KeyType,ValueType>>(1);
    
    _host.getEventLoop().setTimer(new Timer(0,false,new Timer.Callback() {

      @Override
      public void timerFired(Timer timer) {
        try {
          queueClientQueryRequest(queryObject,queryInfo,new QueryCompletionCallback<DataType,KeyType, ValueType>() {

            @Override
            public void queryComplete(QueryRequest<DataType,KeyType,ValueType> request,QueryResult<KeyType, ValueType> queryResult) {
              LOG.info("Recevied QueryComplete for query:" + request.getSourceQuery().getQueryId() + " Object:" + request.getSourceQuery());
              BlockingQueryResult<KeyType,ValueType> result = new BlockingQueryResult<KeyType,ValueType>(queryResult);
              try {
                LOG.info("Queing response for Query:" + request.getSourceQuery().getQueryId() + " Object:" + request.getSourceQuery());
                queue.put(result);
                LOG.info("Queued response for Query:" + request.getSourceQuery().getQueryId() + " Object:" + request.getSourceQuery());
              } catch (InterruptedException e) {
                LOG.error(CCStringUtils.stringifyException(e));
              }
            }

            @Override
            public void queryFailed(QueryRequest<DataType,KeyType,ValueType> request, String reason) {
              LOG.info("Received queryFailed for request:" + request.getSourceQuery().getQueryId()+ " Object:" + request.getSourceQuery());
              BlockingQueryResult<KeyType,ValueType> result = new BlockingQueryResult<KeyType,ValueType>(reason);
              try {
                queue.put(result);
              } catch (InterruptedException e) {
                LOG.error(CCStringUtils.stringifyException(e));
              }
            } 
            
          });
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      } 
      
    }));
    
    try {
      LOG.info("Waiting for Blocking Result for QueryObject:" + queryObject);
      BlockingQueryResult<KeyType,ValueType> result = queue.take();
      LOG.info("Returning result for Object:" + queryObject);
      return result;
    } catch (InterruptedException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
    return null;
  }
  
  long locateMatchingQueryInRequestCollection(Query queryObject,Collection<QueryRequest> collection) {
    Pair<Long,Long> queryCanonicalId = queryObject.getCanonicalQueryId();
    for (QueryRequest request : collection) { 
      Pair<Long,Long> requestCanonicalId = request.getSourceQuery().getCanonicalQueryId();
      LOG.info("Matching " + queryCanonicalId.e0 +":"+queryCanonicalId.e1 + " Against " + requestCanonicalId.e0 +":"+requestCanonicalId.e1);
      if (((long)requestCanonicalId.e0) == (long)queryCanonicalId.e0 && ((long)requestCanonicalId.e1) == (long)queryCanonicalId.e1) {
        LOG.info("Matched! Returning Id:" + request.getSourceQuery().getQueryId());
        return request.getSourceQuery().getQueryId();
      }
    }
    return -1;
  }
  
  long locateMatchingQueryInQueues(Query queryObject) { 
    long queryId = locateMatchingQueryInRequestCollection(queryObject, _queuedClientQueries);
    if (queryId == -1)
      queryId = locateMatchingQueryInRequestCollection(queryObject, _activeRemoteQueries);
    if (queryId == -1)
      queryId = locateMatchingQueryInRequestCollection(queryObject,_activeLocalQueries);
    
    return queryId;
  }
  
  
  @SuppressWarnings("unchecked")
  <DataType extends RPCStruct,KeyType extends WritableComparable,ValueType extends Writable> void queueClientQueryRequest(Query<DataType,KeyType,ValueType> queryObject,ClientQueryInfo theClientRequest,QueryCompletionCallback<DataType,KeyType,ValueType> callback) throws IOException { 
    
    // set query info
    queryObject.setClientQueryInfo(theClientRequest);
        
    // now check cache for persistent query cache info ...
    PersistentQueryInfo persistentQueryInfo = getPersistentQueryInfo(queryObject.getCanonicalQueryId(),theClientRequest.getMaxAge());
    
    boolean cachedQueryAvailable = false;
    
    // ok, cached query found ... 
    if (persistentQueryInfo != null) { 
      LOG.info("Existing Query Id found:" + persistentQueryInfo.getQueryId() + " for Query Request");
      // found cached query... set id of source query object  
      queryObject.setQueryId(persistentQueryInfo.getQueryId());
      // temporarily set the working directory ... 
      Path hdfsWorkingDir = new Path(_hdfsResultsDir,Long.toString(queryObject.getQueryId()));
      // and set it into query common info 
      queryObject.getCommonQueryInfo().setQueryResultPath(hdfsWorkingDir.toString());
      // try to validate it .. 
      if (queryObject.cachedResultsAvailable(_remoteFS, _conf, theClientRequest)) {
        // set flag 
        cachedQueryAvailable = true;
      }
    }
    // IFF cached data is not available, re-run entire query by giving it a new id 
    // (this is to prevent potential collisions in cases where the cached data has 
    //  somehow gone missing, and yet query id is public thus shared across requests)
    boolean existingRequestInProgress = false;
    if (!cachedQueryAvailable) {
      // ok see if we can find it in pending queries list ... 
      long existingIdInQueue = locateMatchingQueryInQueues(queryObject);
      if (existingIdInQueue != -1) { 
        existingRequestInProgress = true;
        LOG.info("Found Existing Query Id: " + existingIdInQueue + " for new Query Request");
        queryObject.setQueryId(existingIdInQueue);
      }
      else { 
        // assign the query a new id 
        queryObject.setQueryId(System.currentTimeMillis());

        LOG.info("Assigning new Id: " + queryObject.getQueryId() + " to Query Request");
      }
      
    }
    
    // establish query hdfs path 
    Path hdfsWorkingDir = new Path(_hdfsResultsDir,Long.toString(queryObject.getQueryId()));
    // create temp dir 
    Path hdfsTempDir = new Path(hdfsWorkingDir,"temp");
    
    if (!existingRequestInProgress) { 
      // create the working directory
      _remoteFS.mkdirs(hdfsWorkingDir);
      _remoteFS.delete(hdfsTempDir, true);
      _remoteFS.mkdirs(hdfsTempDir);
    }

    // establish the hdfs working directory ...
    queryObject.getCommonQueryInfo().setQueryResultPath(hdfsWorkingDir.toString());
    queryObject.getCommonQueryInfo().setQueryTempPath(hdfsTempDir.toString());
    
    // allocate  client request object ...
    QueryRequest<DataType,KeyType,ValueType> clientQueryObj = new QueryRequest<DataType,KeyType,ValueType>(queryObject,theClientRequest,callback);
    
    // setup context ... 
    queryObject.setContext(clientQueryObj);

    LOG.info("Queueing Query Request:" + queryObject.getQueryId() + " SeqNo:" + clientQueryObj.getSequenceNo());

    // add it to queue ... 
    _queuedClientQueries.add(clientQueryObj);
    
    //potentially start the next query ... 
    potentiallyStartNextQuery();
  }
  
  private void publishBindings(Query queryObject)throws IOException {
    // get the canonical id ... 
    Pair<Long,Long> queryCanonicalId = queryObject.getCanonicalQueryId();
    
    // and store the relationship in hdfs file 
    Path canonicalToIdBinding = new Path(_hdfsQueryHistoryDir,canonicalIdAndQueryIdToFilename(queryCanonicalId,queryObject.getQueryId()));
    if (!_remoteFS.exists(canonicalToIdBinding)) {
      
      LOG.info("Binding CanonicalQuery Id:[" 
          + queryCanonicalId.e0
          +":"
          + queryCanonicalId.e1
          + "] to QueryId:" + queryObject.getQueryId() + " via path:" + canonicalToIdBinding);
      
      _remoteFS.createNewFile(canonicalToIdBinding);
    }
  }
  
  @SuppressWarnings("unchecked")
  private void potentiallyStartNextQuery()throws IOException { 
    
    FileSystem fileSystem = _remoteFS;
    
    if (_queuedClientQueries.size() != 0) { 
    
      ArrayList<QueryRequest> requests = new ArrayList<QueryRequest>(_queuedClientQueries);
      
      for (QueryRequest request : requests) {
      //while (_queuedClientQueries.size() != 0 && _activeClientQueries.size() < _maxConcurrentQueries) { 
  
        //QueryRequest request = _queuedClientQueries.poll();
        
        //LOG.info("Processing Query:" + request.getSourceQuery().getQueryId() + " SeqNo:" + request.getSequenceNo() +  " ActiveCount:" + _activeClientQueries.size());
       
        int localDispatchSlotsAvail = _maxConcurrentQueries - _activeLocalQueries.size();
        int remoteDispatchSlotsAvail = _maxConcurrentQueries - _activeLocalQueries.size();
        
        try {
          // first see if a remote (or local) query is active ... 
          if (_activeRemoteOrLocalQueries.get(request.getSourceQuery().getQueryId())  != null) {
            //LOG.info("Cannot Dispatch ClientRequest:" + request.getClientQueryInfo().getClientQueryId() + " because existing query in progress");
          }
          else {
            ArrayList<QueryInputSplit> requiredSplits = new ArrayList<QueryInputSplit>();
            // first check to see if cached results are available ... 
            if (request.getSourceQuery().cachedResultsAvailable(fileSystem,_conf,request.getClientQueryInfo())) {
              // remove from queued 
              _queuedClientQueries.remove(request);
              // ok publish bindings between canonical query id and the canonical query id (if necessary)
              publishBindings(request.getSourceQuery());
              // add to active ... 
              //_activeCacheQueries.add(request);
              LOG.info("Running Cache Query for Query:" + request.getSourceQuery().getQueryId());            
              runCacheQuery(request);
            }
            // check to see if remote dispatch is required ..
            else if (request.getSourceQuery().requiresShardedQuery(fileSystem,_conf,this,request,getHost().getShardCountForQuery(request.getSourceQuery()),requiredSplits)) {
              if (remoteDispatchSlotsAvail != 0) {
                _queuedClientQueries.remove();
                // ok, we need at least on shard to run on ... 
                if (requiredSplits.size() == 0) { 
                    LOG.error("Query:" + request.getSourceQuery().getQueryId() + " FAILED WITH EMPTY REQUIRED SHARDS LIST");
                    throw new IOException("Empty Host List prior to remoteDispath!");
                }
                // set shard id to host mapping into query 
                request.getSourceQuery().setSplits(requiredSplits);
                // ok, we ready for remote dispatch ... 
                // add to active ... 
                _activeRemoteQueries.add(request);
                // add to remote dispatch id set 
                _activeRemoteOrLocalQueries.put(request.getSourceQuery().getQueryId(),request);
                LOG.info("Running Remote Query for Query:" + request.getSourceQuery().getQueryId());
                // and dispatch request .. 
                runRemoteQuery(fileSystem,request);
              }
            }
            // otherwise .. run Local Request
            else {
              if (localDispatchSlotsAvail != 0) {
                _queuedClientQueries.remove(request);
                // add to active ... 
                _activeLocalQueries.add(request);
                // add to remote dispatch id set 
                _activeRemoteOrLocalQueries.put(request.getSourceQuery().getQueryId(),request);
                LOG.info("Running Local Query for Query:" + request.getSourceQuery().getQueryId());
                // and dispatch request .. 
                runLocalQuery(request);
              }
            }
          }
        }
        catch (IOException e) {
          // remove from queued ... 
          _queuedClientQueries.remove(request);
          // log error 
          LOG.error("Client Request:" + request.getClientQueryInfo().getClientQueryId() + " Failed with Exception:" + CCStringUtils.stringifyException(e));
          // and fail outright
          request.getCompletionCallback().queryFailed(request,CCStringUtils.stringifyException(e));
        }
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  private void deactivateRequest(QueryRequest request,RunState newRunState) { 
    LOG.info("DeActivating Query:" + request.getSourceQuery().getQueryId() + " RunState:" + request.getRunState().toString());
    // preserve last run state ... 
    RunState lastRunState = request.getRunState();
    // set new run state 
    request.setRunState(newRunState);
    // remote from active or remote list 
    _activeRemoteOrLocalQueries.remove(request.getSourceQuery().getQueryId());
    
    boolean requeue = false;
    if (lastRunState == RunState.RUNNING_REMOTE || lastRunState == RunState.RUNNING_LOCAL || lastRunState == RunState.ERROR) { 
      _activeRemoteOrLocalQueries.remove(request.getSourceQuery().getQueryId());
      if (lastRunState == RunState.RUNNING_REMOTE) 
        _activeRemoteQueries.remove(request);
      else if (lastRunState == RunState.RUNNING_LOCAL)
        _activeLocalQueries.remove(request);
      if (request.getRunState() == RunState.IDLE) { 
        LOG.info("Marking Query:" + request.getSourceQuery().getQueryId() + " SeqNo:" + request.getSequenceNo() + " for Requeue");
        requeue = true;
      }
    }

    Path hdfsQueryTempDir = new Path(request.getSourceQuery().getCommonQueryInfo().getQueryTempPath());
    
    // first things first, delete temp file!!!
    File queryTempFile = getTempDirForQuery(request.getSourceQuery().getQueryId());
    LOG.info("** Deleting Temp File for Query:" + request.getSourceQuery().getQueryId() + " At:" + queryTempFile.getAbsolutePath());
    FileUtils.recursivelyDeleteFile(queryTempFile);
    LOG.info("** Deleting HDFS Temp Dir:" + hdfsQueryTempDir + " for Query:" + request.getSourceQuery().getQueryId());
    try {
      _remoteFS.delete(hdfsQueryTempDir,true);
    } catch (IOException e1) {
      LOG.error(CCStringUtils.stringifyException(e1));
    }

    if (request.getRunState() == RunState.ERROR) { 
      Path hdfsWorkingDir = new Path(request.getSourceQuery().getCommonQueryInfo().getQueryResultPath());
      LOG.info("** Query FAILED. Deleting HDFS Working Dir:" + hdfsWorkingDir  + " for Query:" + request.getSourceQuery().getQueryId());
      try {
        _remoteFS.delete(hdfsWorkingDir,true);
      } catch (IOException e1) {
        LOG.error(CCStringUtils.stringifyException(e1));
      }
    }
    
    if (requeue && request.getRunState() == RunState.IDLE) { 
      LOG.info("ReQueueing Query:" + request.getSourceQuery().getQueryId() + " SeqNo:" + request.getSequenceNo());
      _queuedClientQueries.add(request);
    }
    try {
      LOG.info("** Restarting Next Query");
      potentiallyStartNextQuery();
    } catch (IOException e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
  
  @SuppressWarnings("unchecked")
  private void requeueRequest(QueryRequest request) {
    deactivateRequest(request,RunState.IDLE);
  }
  
  @SuppressWarnings("unchecked")
  private void runRemoteQuery(final FileSystem remoteFileSystem,final QueryRequest request) { 
    
    //LOG.info("runRemoteQuery Called for Query:" + request.getSourceQuery().getQueryId());
    request.setRunState(QueryRequest.RunState.RUNNING_REMOTE); 
    request.getQueryStatus().setProgress(0.0f);
    
    try { 
      request.getSourceQuery().startShardedQuery(this,request,request.getSourceQuery().getSplits(), 
          new QueryProgressCallback() {

            @Override
            public boolean updateProgress(Query theQueryObject,float percentComplete) {
              LOG.info("Got updateProgress callback for:" + theQueryObject.getQueryId());
              return true;
            } 
        
          },
          
          new RemoteQueryCompletionCallback() {

            @Override
            public void queryComplete(Query query, long resultCount) {
              LOG.info("Recevied QueryComplete for Query:" + request.getSourceQuery().getQueryId());
              
              // call remote dispatch complete
              try { 
                query.shardedQueryComplete(remoteFileSystem,_conf,request,resultCount);
                
                if (resultCount > 0) { 
                    LOG.info("Remote Query:" + request.getSourceQuery().getQueryId() + " returned:" + resultCount  + " results");
                    // deactive request first ...
                    requeueRequest((QueryRequest)query.getContextObject());
                }
                else {
                    
                    LOG.info("Query:" + request.getSourceQuery().getQueryId() + " returned zero results");
                    deactivateRequest(request,RunState.ERROR);
                    
                    QueryResult result = new QueryResult();
                    result.setTotalRecordCount(0);
                    request.getCompletionCallback().queryComplete(request, result);
                    
                    LOG.info("Query:" + request.getSourceQuery().getQueryId() + " DONE DUDE");                  
                }
              }
              catch (IOException e) {
                String error = "Query: " + request.getSourceQuery().getQueryId() + " Failed with Exception:" + CCStringUtils.stringifyException(e); 
                LOG.error(error);
                
                // deactivate the request 
                deactivateRequest((QueryRequest)query.getContextObject(),RunState.ERROR);
                
                request.getCompletionCallback().queryFailed(request, error);
              }
            }

            @Override
            public void queryFailed(Query query, final String reason) {
              LOG.info("Recevied QueryFailed for Query:" + request.getSourceQuery().getQueryId() + " Reason:" + reason);
              // inform query of failure 
              query.shardedQueryFailed(remoteFileSystem);
              // deactivate the request 
              deactivateRequest((QueryRequest)query.getContextObject(),RunState.ERROR);
              
              request.getCompletionCallback().queryFailed(request, reason);
            } 
          });
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      deactivateRequest(request,RunState.ERROR);
      request.getCompletionCallback().queryFailed(request, CCStringUtils.stringifyException(e));
    }
  }
  
  @SuppressWarnings("unchecked")
  private void runLocalQuery(final QueryRequest request) {
    //LOG.info("runLocalQuery Called for Query:" + request.getSourceQuery().getQueryId());

    request.setRunState(QueryRequest.RunState.RUNNING_LOCAL);
    
    request.getSourceQuery().startLocalQuery(
        _remoteFS,
        _conf,
        getTempDirForQuery(request.getSourceQuery().getQueryId()),          
        getHost().getEventLoop(),
        request,
        new RemoteQueryCompletionCallback() {

          @Override
          public void queryComplete(Query query, long resultCount) {
            LOG.info("Recevied QueryComplete for Query:" + request.getSourceQuery().getQueryId());
            if (resultCount > 0) {
              LOG.info("Local Query:" + request.getSourceQuery().getQueryId() + " returned:" + resultCount  + " results");
              // requeue request ...
              requeueRequest((QueryRequest)query.getContextObject());
            }
            else {
              LOG.info("Query:" + request.getSourceQuery().getQueryId() + " returned zero results");
              // deactive ... 
              deactivateRequest((QueryRequest)query.getContextObject(),RunState.ERROR);
              
              // initiate callback 
              
              QueryResult result = new QueryResult();
              result.setTotalRecordCount(0);
              request.getCompletionCallback().queryComplete(request, result);
              LOG.info("Query:" + request.getSourceQuery().getQueryId() + " DONE DUDE");
            }
          }

          @Override
          public void queryFailed(Query query, final String reason) {
            LOG.info("Recevied QueryFailed for Query:" + request.getSourceQuery().getQueryId() + " Reason:" + reason);
            deactivateRequest((QueryRequest)query.getContextObject(),RunState.ERROR);
            request.getCompletionCallback().queryFailed(request, reason);
          } 
        });
  }
  
  @SuppressWarnings("unchecked")
  private void runCacheQuery(QueryRequest request) {
    //LOG.info("runCacheQuery Called for Query:" + request.getSourceQuery().getQueryId());
    request.setRunState(QueryRequest.RunState.RUNNING_CACHE);
    
    request.getSourceQuery().startCacheQuery(
        this,
        _remoteFS,
        _conf,
        getHost().getEventLoop(),
        request,
        new QueryCompletionCallback() {

          @Override
          public void queryComplete(QueryRequest request,QueryResult queryResult) {
            deactivateRequest(request,RunState.IDLE);
            request.getCompletionCallback().queryComplete(request, queryResult);
          }

          @Override
          public void queryFailed(QueryRequest request, String reason) {
            deactivateRequest(request,RunState.ERROR);
            request.getCompletionCallback().queryFailed(request, reason);
          } 
        }
        );
  }
  
  private static String canonicalIdAndQueryIdToFilename(Pair<Long,Long> canonicalId,long queryId) { 
    return canonicalIdToString(canonicalId) + "-" + Long.toString(queryId);
  }
  private static String canonicalIdToString(Pair<Long,Long> canonicalId) { 
    return Long.toHexString(canonicalId.e0) + "-" + Long.toHexString(canonicalId.e1);
  }
  
  private static long getTimestampFromQueryPathString(String encodedQueryFilename) { 
    return Long.parseLong(encodedQueryFilename.substring(encodedQueryFilename.lastIndexOf("-") + 1));
  }
  
  private static String getCanonicalIdFromPathString(String encodedQueryFilename) { 
    return encodedQueryFilename.substring(0,encodedQueryFilename.lastIndexOf("-"));
  }
  
  private Path persistentQueryInfoToPath(PersistentQueryInfo queryInfo) {
    return new Path(_hdfsQueryHistoryDir,queryInfo.getCannonicalQueryId() + "-" + queryInfo.getQueryId());
  }
  
  private PersistentQueryInfo getPersistentQueryInfo(Pair<Long,Long> queryCanonicalId,long maxAge) throws IOException {
    // construct pattern 
    Path searchPattern = new Path(_hdfsQueryHistoryDir,canonicalIdToString(queryCanonicalId) + "-*");
    // search for it 
    FileStatus[] matches = _remoteFS.globStatus(searchPattern);
    FileStatus bestMatch = null;
    for (FileStatus match : matches) { 
      if (bestMatch == null) {
        bestMatch = match;
      }
      else {
        long bestMatchTS = getTimestampFromQueryPathString(bestMatch.getPath().getName());
        long matchTS     = getTimestampFromQueryPathString(match.getPath().getName());
        if (matchTS > bestMatchTS)
          bestMatch = match;
      }
    }
    
    if (bestMatch != null) {
      
      PersistentQueryInfo queryInfo = new PersistentQueryInfo();

      queryInfo.setCannonicalQueryId(getCanonicalIdFromPathString(bestMatch.getPath().getName()));
      queryInfo.setQueryId(getTimestampFromQueryPathString(bestMatch.getPath().getName()));
      
      // if time elapsed since query creation time is less than maxQueryAge threshold ... 
      if (System.currentTimeMillis() - queryInfo.getQueryId() < maxAge) { 
        return queryInfo;
      }
    }
    return null;
  }
  
  private File getTempDirForQuery(long queryId) { 
    return new File(_localTempDir,Long.toString(queryId));
  }
  
  public void renderQueueStats(final JsonWriter jsonWriter)throws IOException { 
    EventLoop eventLoop = getHost().getEventLoop();
    final AtomicReference<IOException> exceptionRef = new AtomicReference<IOException>();
    if (Thread.currentThread() != eventLoop.getEventThread()) { 
      final Semaphore semaphore = new Semaphore(0);
      eventLoop.queueAsyncCallback(new Callback() {

        @Override
        public void execute() {
          try { 
            renderQueueStatsInternal(jsonWriter);
          }
          catch (IOException e) { 
            exceptionRef.set(e);
          }
          finally { 
            semaphore.release();
          }
        } 
        
      });
      semaphore.acquireUninterruptibly();
      if (exceptionRef.get() != null) { 
        throw exceptionRef.get();
      }
    }
    else { 
      renderQueueStatsInternal(jsonWriter);
    }
  }
  
  private void renderQueueStatsInternal(JsonWriter writer)throws IOException { 
    writer.beginObject();
    writer.name("pending").beginArray();
    for (QueryRequest request : _queuedClientQueries) { 
      writer.beginObject();
      writer.name("id:").value(request.getSourceQuery().getQueryId());
      writer.name("state").value(request.getRunState().toString());
      writer.name("pctComplete").value(request.getQueryStatus().getProgress());
      writer.name("type").value(request.getSourceQuery().getQueryDomainId());
      writer.name("params").value(request.getSourceQuery().getUniqueQueryParameters());
      writer.endObject();
    }
    writer.endArray();
    writer.name("running").beginArray();
    for (QueryRequest request : _activeRemoteOrLocalQueries.values()) { 
      writer.beginObject();
      writer.name("id:").value(request.getSourceQuery().getQueryId());
      writer.name("state").value(request.getRunState().toString());
      writer.name("pctComplete").value(request.getQueryStatus().getProgress());
      writer.name("type").value(request.getSourceQuery().getQueryDomainId());
      writer.name("params").value(request.getSourceQuery().getUniqueQueryParameters());
      writer.endObject();
    }
    writer.endArray();
    writer.endObject();
  }
  
  
  
}
