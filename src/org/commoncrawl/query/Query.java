package org.commoncrawl.query;

import java.io.File;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.hadoop.mergeutils.MergeSortSpillWriter;
import org.commoncrawl.hadoop.mergeutils.RawKeyValueComparator;
import org.commoncrawl.hadoop.mergeutils.SequenceFileMerger;
import org.commoncrawl.hadoop.mergeutils.SequenceFileSpillWriter;
import org.commoncrawl.hadoop.mergeutils.SpillWriter;
import org.commoncrawl.rpc.BinaryProtocol;
import org.commoncrawl.rpc.OutgoingMessageContext;
import org.commoncrawl.rpc.RPCStruct;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.FPGenerator;
import org.commoncrawl.util.shared.FileUtils;
import org.commoncrawl.util.shared.FlexBuffer;
import org.commoncrawl.util.shared.Tuples.Pair;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;





@SuppressWarnings("unchecked")
public abstract class Query<DataType extends RPCStruct,ResultKeyType extends WritableComparable,ResultValueType extends Writable> {
  
  private static final Log LOG = LogFactory.getLog(Query.class);
  
  private class ShardState {
    
    public ShardState(QueryStatus queryStatus,RemoteQueryInfo queryInfo) { 
      _queryStatus = queryStatus;
      _queryInfo = queryInfo; 
    }
    
    public void setAssignedNode(QuerySlaveConnection node) { 
      _assignedNode = node;
    }
    
    private QuerySlaveConnection  _assignedNode;
    private QueryStatus           _queryStatus;
    private RemoteQueryInfo       _queryInfo;
    public  boolean               _logged = false;
    
    public QuerySlaveConnection getAssignedNode() { return _assignedNode; } 
    public QueryStatus          getQueryStatus() { return _queryStatus; }
    public RemoteQueryInfo      getQueryInfo() { return _queryInfo; }
  }
  
  protected static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  protected RemoteQueryInfo _remoteQueryInfo;
  protected ClientQueryInfo _clientQueryInfo;
  protected QueryCommon     _commonInfo;
  protected DataType        _queryData;
  private   Object          _contextObj;
  private   QueryController _queryController;
  protected QueryStatus     _queryStatus;
  protected Timer           _queryMonitorTimer;
  protected QuerySlaveServer _slave;
  protected Thread           _queryThread;
  protected boolean          _cancelQuery;
  protected HashMap<Integer,ShardState> _shardStatusMap = null;
  protected RemoteQueryCompletionCallback _completionCallback;
  protected ArrayList<QueryInputSplit> _inputSplits = new ArrayList<QueryInputSplit>();
  // query request associated with master query object...
  protected QueryRequest    _associatedRequest;

  
  /** master side query object constructor **/
  public Query(DataType queryData) {
    _queryData = queryData;
    _commonInfo = new QueryCommon();
    _queryStatus = new QueryStatus();
    _queryStatus.setStatus(QueryStatus.Status.PENDING);
  }
  
  protected void setQueryData(DataType queryData) { _queryData = queryData; }
  protected DataType getQueryData() { return _queryData; }
  
  
  /** remote query initialization *
   * @return */
  public void initializeRemoteQuery(QuerySlaveServer slave,RemoteQueryInfo queryInfo,DataType queryData) { 
    try {
      _slave = slave;
      _remoteQueryInfo  = queryInfo;
      _clientQueryInfo  = queryInfo.getClientQueryData();
      _commonInfo = (QueryCommon)queryInfo.getCommonInfo().clone();
      _queryData  = queryData;
      _queryStatus = new QueryStatus();
      _queryStatus.setQueryId(_commonInfo.getQueryId());
      _queryStatus.setShardId(_remoteQueryInfo.getShardId());
    } catch (CloneNotSupportedException e) {
    }
  }
  
  public RemoteQueryInfo getRemoteQueryInfo() { return _remoteQueryInfo; }
  
  /** get query status **/
  public QueryStatus getQueryStatus() { return _queryStatus; }
  
  // get the common query info 
  public  QueryCommon getCommonQueryInfo() { return _commonInfo; }
  
  // get client query info 
  public ClientQueryInfo getClientQueryInfo() { return _clientQueryInfo; }
  // set client query info 
  public void setClientQueryInfo(ClientQueryInfo info) { _clientQueryInfo = info; } 
  
  // get shard id to host mapping - only valid at master 
  public ArrayList<QueryInputSplit> getSplits() { return _inputSplits; }
  // set shard id to host mapping - only set by master 
  public void setSplits(ArrayList<QueryInputSplit> splits) { _inputSplits = splits; } 
  
  public Object getContextObject() { return _contextObj; }
  public void   setContext(Object context) { _contextObj = context; }
  
  /** start a slave query thread **/
  public void runShardedQuery(
      final String fqHostName,
      final FileSystem remoteFileSystem,
      final Configuration conf,
      final EventLoop eventLoop,
      final QuerySlaveServer server,
      final File queryTempDir,
      final QueryProgressCallback<DataType,ResultKeyType,ResultValueType> progressCallback,
      final RemoteQueryCompletionCallback completionCallback) throws IOException {
    
    synchronized (Query.this) {
      // update query status ...
      _queryStatus.setStatus(QueryStatus.Status.RUNNING);
    }
    // delete the tempfile directory ...
    FileUtils.recursivelyDeleteFile(queryTempDir);
    // and recreate it ... 
    queryTempDir.mkdirs();
    // ok get hdfs attempt path ... 
    final Path hdfsAttemptPath = getHDFSQueryAttemptFilePathForShard(getRemoteQueryInfo().getShardId());
    // create it 
    remoteFileSystem.mkdirs(hdfsAttemptPath);
    // create merge temp
    final Path mergeTemp  = new Path(hdfsAttemptPath,"mergeTemp");
    remoteFileSystem.mkdirs(mergeTemp);
    // create region data filename 
    final Path shardDataFile = new Path(hdfsAttemptPath,getPartNameForSlave(getRemoteQueryInfo().getShardId()));
    
    _queryThread = new Thread(new Runnable() {

      @Override
      public void run() {
        try {
		  
          CompressionCodec codec = null;
          Class<? extends CompressionCodec> codecClass = conf.getClass("mapred.output.compression.codec", null, CompressionCodec.class);
          if (codecClass != null) {
            codec = ReflectionUtils.newInstance(codecClass,conf);
          }
		
          LOG.info(fqHostName + " Creating SpillWriter with output file:" + shardDataFile +" for Query:" + getQueryId() + " Codec is:" + codecClass);
          SequenceFileSpillWriter<ResultKeyType, ResultValueType> finalSpillWriter = new SequenceFileSpillWriter<ResultKeyType, ResultValueType>(
              remoteFileSystem,
              conf,
              shardDataFile, getKeyClass(), getValueClass(), null, codec,(short)2);
          
          LOG.info(fqHostName + " Creating MergingSpillWriter at:" + hdfsAttemptPath + " for Query:" + getQueryId());
          
          try { 
            // create spill writer ... 
            MergeSortSpillWriter<ResultKeyType,ResultValueType> mergingSpillWriter = new MergeSortSpillWriter<ResultKeyType,ResultValueType>(
                conf, 
                finalSpillWriter, 
                remoteFileSystem,
                mergeTemp, 
                null,
                allocateRawComparator(), 
                getKeyClass(), 
                getValueClass(), 
                null, 
                null);
            
            try { 
              LOG.info(fqHostName + " Slave Query Thread:Executing for Query:" + getQueryId());
              // execute the specific query object.
              long resultCount = executeShardedQuery(remoteFileSystem,conf,eventLoop,server,queryTempDir,hdfsAttemptPath,mergingSpillWriter,new QueryProgressCallback<DataType,ResultKeyType,ResultValueType>() {
    
                @Override
                public boolean updateProgress(final Query<DataType,ResultKeyType,ResultValueType> theQueryObject,float percentComplete) {
                  synchronized (Query.this) {
                    if (_cancelQuery) {
                      _queryStatus.setStatus(QueryStatus.Status.CANCELLED);
                      return false;
                    }
                    else { 
                      _queryStatus.setProgress(percentComplete);
                    }
                  }
                  return progressCallback.updateProgress(theQueryObject,percentComplete);
                } 
                
              });
              
              // ok close spill objects here ... 
              if (mergingSpillWriter != null) { 
                mergingSpillWriter.close();
                mergingSpillWriter = null;
              }
              
              if (finalSpillWriter != null) { 
                finalSpillWriter.close();
                finalSpillWriter = null;
              }
              
              // delete merge temp ... 
              remoteFileSystem.delete(mergeTemp,true);
              
              // udpate query status 
              synchronized (Query.this) {
                if (_queryStatus.getStatus() != QueryStatus.Status.CANCELLED) { 
                  _queryStatus.setStatus(QueryStatus.Status.FINISHED);
                  _queryStatus.setOptResultCount(resultCount);
                  // copy everything in temp output to final output path ... 
                  Path finalResultsPath = getHDFSQueryResultsPath();
                  
                  FileStatus[] resultingFiles = remoteFileSystem.globStatus(new Path(hdfsAttemptPath,"*"));
                  for (FileStatus resultsFile : resultingFiles) { 
                    boolean success = remoteFileSystem.rename(resultsFile.getPath(),new Path(finalResultsPath,resultsFile.getPath().getName()));
                    if (!success) { 
                      LOG.error(fqHostName + " Failed to Copy Result File:" + resultsFile.getPath() 
                          + " to destination:" + finalResultsPath 
                          + " for Query:" + getQueryId() + " Shard:" + getRemoteQueryInfo().getShardId());
                    }
                    else { 
                      LOG.info(fqHostName + " Copied Results File:" + resultsFile.getPath() 
                          + " to destination:" + finalResultsPath 
                          + " for Query:" + getQueryId() + " Shard:" + getRemoteQueryInfo().getShardId());
                      
                    }
                  }
                }
              }
              
              if (eventLoop != null) { 
                final long finalResultCount = resultCount;
                //schedule call asynchronously ... 
                eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
    
                  @Override
                  public void timerFired(Timer timer) {
                    // execute directly from thread ... 
                    LOG.info(fqHostName + " Query:" + getQueryId() +" Completed with:" + finalResultCount + " results.");
                    // and complete... 
                    completionCallback.queryComplete(Query.this,finalResultCount);
                  } 
                }));
              }
              else {
                // execute directly from thread ... 
                LOG.info(fqHostName + " Query:" + getQueryId() +" Completed with:" + resultCount + " results.");
                // and complete... 
                completionCallback.queryComplete(Query.this,resultCount);
              }
            }
            finally { 
              if (mergingSpillWriter != null) { 
                try {
                  mergingSpillWriter.close();
                } catch (IOException e) {
                  LOG.error(CCStringUtils.stringifyException(e));
                }
              }
            }
          }
          finally { 
            if (finalSpillWriter != null) { 
              finalSpillWriter.close();
            }
          }
        }
        catch (final Exception e){
          
          synchronized (Query.this) {
            _queryStatus.setStatus(QueryStatus.Status.ERROR);
            _queryStatus.setOptErrorReason(CCStringUtils.stringifyException(e));
          }
          
          if (eventLoop != null) { 
            //schedule call asynchronously ... 
            eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {

              @Override
              public void timerFired(Timer timer) {
                LOG.error(CCStringUtils.stringifyException(e));
                completionCallback.queryFailed(Query.this, "Query:" + getQueryId() +" Failed on Host:" + fqHostName + " with Exception:" + CCStringUtils.stringifyException(e));
              } 
            }));
          }
          else {
            //execute callback in current thread's context
            LOG.error(CCStringUtils.stringifyException(e));
            completionCallback.queryFailed(Query.this, "Query:" + getQueryId() +" Failed on Host:" + fqHostName + " with Exception:" + CCStringUtils.stringifyException(e));
          }
        }
        finally { 
          // delete temp file directory no matter what 
          FileUtils.recursivelyDeleteFile(queryTempDir);
          // and delete hdfs temp dir too 
          try {
            remoteFileSystem.delete(hdfsAttemptPath,true);
          } catch (IOException e) {
            LOG.error(fqHostName + " Failed to delete attempt directory:" + hdfsAttemptPath 
                + " for Query:" + getQueryId() + " Shard:" + getRemoteQueryInfo().getShardId());
          }
        }
        
      } 
    });
    
    LOG.info(fqHostName + " Starting Slave Query Thread for Query:" + getQueryId());
    _queryThread.start();
  }
  
  /** start client query thread**/
  public void startLocalQuery(final FileSystem fileSystem,final Configuration conf,final File tempFileDir,final EventLoop eventLoop,final QueryRequest<DataType,ResultKeyType,ResultValueType> queryRequest,final RemoteQueryCompletionCallback completionCallback) {

    // set up appropriate status ... 
    queryRequest.getQueryStatus().setStatus(QueryStatus.Status.RUNNING);
    queryRequest.getQueryStatus().setProgress(.66f);
    
    _queryThread = new Thread(new Runnable() {

      @Override
      public void run() {
        LOG.info("Client Query Thread.Executing for Query:" + getQueryId());
        // execute the specific query object.
        try {
          // create temp file dir ... 
          FileUtils.recursivelyDeleteFile(tempFileDir);
          
          LOG.info("Client Query Thread for:" + getQueryId() +" creating temp file directory:" + tempFileDir.getAbsolutePath());
          tempFileDir.mkdirs();
          
          LOG.info("Executing Local Query for Query:" + getQueryId());
          // execute local query 
          final long resultCount = executeAggregationQuery(fileSystem,conf,eventLoop,tempFileDir,queryRequest,
              new QueryProgressCallback<RPCStruct, WritableComparable, Writable>() {

                @Override
                public boolean updateProgress(Query<RPCStruct, WritableComparable, Writable> theQueryObject,float percentComplete) {
                  
                  queryRequest.getQueryStatus().setProgress(.66f + (.33f * percentComplete));
                  
                  return true;
                }
              });
          // update status ? 
          queryRequest.getQueryStatus().setProgress(.99f);

          // and callback in async thread context...
          eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
            @Override
            public void timerFired(Timer timer) {
              LOG.info("Local QueryComplete for Query:" + getQueryId());
              completionCallback.queryComplete(queryRequest.getSourceQuery(),resultCount);
            }
          }));
          
        } catch (final IOException e) {
          LOG.error("Query: " + getQueryId() + " Failed on executeLocal with Error:" + CCStringUtils.stringifyException(e));
          final String error = CCStringUtils.stringifyException(e);
          eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
            @Override
            public void timerFired(Timer timer) {
              completionCallback.queryFailed(queryRequest.getSourceQuery(),error);
            }
          }));
        }
      } 
    });
    
    LOG.info("Starting Local Query Thread for Query:" + getQueryId());
    _queryThread.start();
  }

  /** start client query thread**/
  public void startCacheQuery(final QueryController controller,final FileSystem fileSystem,final Configuration conf,final EventLoop eventLoop,final QueryRequest<DataType,ResultKeyType,ResultValueType> queryRequest,final QueryCompletionCallback<DataType,ResultKeyType,ResultValueType> completionCallback) {

    // set up appropriate status ... 
    queryRequest.getQueryStatus().setStatus(QueryStatus.Status.RUNNING);
    queryRequest.getQueryStatus().setProgress(.99f);
    
    _queryThread = new Thread(new Runnable() {

      @Override
      public void run() {
        LOG.info("Executing Cache Query for Query:" + getQueryId());
        // execute the specific query object.
        try {
          // execute cache query 
          executeCacheQuery(fileSystem,conf,eventLoop,controller,queryRequest, new QueryCompletionCallback<DataType,ResultKeyType,ResultValueType>() {

            @Override
            public void queryComplete(final QueryRequest<DataType,ResultKeyType,ResultValueType> request,final QueryResult<ResultKeyType,ResultValueType> queryResult) {
              // and call outer callback in async thread context...
              eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
                @Override
                public void timerFired(Timer timer) {
                  queryRequest.getQueryStatus().setProgress(1.0f);
                  LOG.info("Calling queryComplete on cacheRequest for Query:" + getQueryId());
                  completionCallback.queryComplete(queryRequest,queryResult);
                  //LOG.info("Finished Calling queryComplete for Query:" + getQueryId());

                }
              }));
            }

            @Override
            public void queryFailed(final QueryRequest<DataType,ResultKeyType,ResultValueType> request, final String reason) {
              // and call outer callback in async thread context...
              eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
                @Override
                public void timerFired(Timer timer) {
                  LOG.info("Calling queryFailed on cacheRequest for Query:" + getQueryId() + " Reason:" + reason);
                  completionCallback.queryFailed(queryRequest,reason);
                  //LOG.info("Finished Calling queryFailed for Query:" + getQueryId());
                }
              }));
            }


          });
          
        } catch (final IOException e) {
          LOG.error("Query: " + getQueryId() + " Failed on cacheQuery with Error:" + CCStringUtils.stringifyException(e));
          final String error = CCStringUtils.stringifyException(e);
          eventLoop.setTimer(new Timer(0,false,new Timer.Callback() {
            @Override
            public void timerFired(Timer timer) {
              completionCallback.queryFailed(queryRequest,error);
            }
          }));
        }
      } 
    });
    
    LOG.info("Starting Cache Query Thread for Query:" + getQueryId());
    _queryThread.start();
  }  
  
  
  
  /** start the remote query **/
  public void startShardedQuery(
      QueryController controller,
      final QueryRequest request,
      ArrayList<QueryInputSplit> inputSplits,
      final QueryProgressCallback<DataType,ResultKeyType,ResultValueType> progressCallback, 
      final RemoteQueryCompletionCallback completionCallback ) throws IOException {
    
    LOG.info("Starting Remote(Master)Query for Query:" + getQueryId());
    
    // save a refrerence to the controller 
    _queryController = controller;
    // set request object 
    _associatedRequest = request;
    // store completion callback 
    _completionCallback = completionCallback;
    synchronized(Query.this) {
      // update query status ...
      _queryStatus.setStatus(QueryStatus.Status.RUNNING);
    }
    
    // allocate query state vector 
    _shardStatusMap = new HashMap<Integer,ShardState>();
    
    // iterate splits ...  
    for (QueryInputSplit split : inputSplits) {
      LOG.info("Dispatching Query:" + getQueryId() + " to Shard:" + split.getShardId());
      
      // construct a query details object 
      RemoteQueryInfo queryDetails= new RemoteQueryInfo();
      
      // populate it ... 
      queryDetails.setCommonInfo(getCommonQueryInfo());
      // populate client query info 
      queryDetails.setClientQueryData(getClientQueryInfo());
      // set class info based on 'this'
      queryDetails.setQueryClassType(getClass().getName());
      // set data type
      queryDetails.setQueryDataClassType(_queryData.getClass().getName());
      // marshall data buffer
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      _queryData.serialize(outputBuffer,new BinaryProtocol());
      // and add it to query details
      queryDetails.setQueryDataBuffer(new FlexBuffer(outputBuffer.getData(),0,outputBuffer.getLength(),true));
      // set shard id 
      queryDetails.setShardId(split.getShardId());
      
      // allocate a new query status 
      QueryStatus queryStatus = new QueryStatus();
      // setup basic structure ... 
      queryStatus.setQueryId(getQueryId());
      queryStatus.setShardId(split.getShardId());
      queryStatus.setAttempts(0);
      queryStatus.setStatus(QueryStatus.Status.PENDING);
      // allocate super structure 
      ShardState statusInfo = new ShardState(queryStatus,queryDetails);
      // add to map  
      _shardStatusMap.put(queryStatus.getShardId(),statusInfo);
    }
    
    // start a monitor timer 
    _queryMonitorTimer = new Timer(500,true,new Timer.Callback() {
      
      @Override
      public void timerFired(Timer timer) {
        try {
          updateRemoteQueryStatus(request);
        } catch (IOException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    });
    
    _queryController.getHost().getEventLoop().setTimer(_queryMonitorTimer);
    
    LOG.info("Dispatching Slave Queries for Query:" + getQueryId());
    updateRemoteQueryStatus(request);
    
    
  }
  
  public void updateQueryStatus(QueryStatus statusUpdate) throws IOException{
  	ShardState slaveInfo = _shardStatusMap.get(statusUpdate.getShardId());
  	
    if (slaveInfo != null) { 
      //LOG.info("Recevied Query Status Update for Query:" + getQueryId() + " Slave:" + slaveInfo.getOnlineState().getFullyQualifiedName());
      if (statusUpdate.getStatus() == QueryStatus.Status.ERROR && slaveInfo._queryStatus.getStatus() != QueryStatus.Status.ERROR) { 
        LOG.info("Slave:" + slaveInfo.getAssignedNode().getFullyQualifiedName() + " Reported Error:" + statusUpdate.getOptErrorReason() + " for Query:" + getQueryId());
      }
      else if (statusUpdate.getStatus() == QueryStatus.Status.FINISHED && slaveInfo._queryStatus.getStatus() != QueryStatus.Status.FINISHED) { 
        LOG.info("Slave:" + slaveInfo.getAssignedNode().getFullyQualifiedName() + " Reported FINISHED for Query:" + getQueryId() + " ResultCount:" + statusUpdate.getOptResultCount());
      }
      try {
        // clear some bits to prevent accidental overwrite of important master side 
        // fields :-(
        statusUpdate.setFieldClean(QueryStatus.Field_ATTEMPTS);
        statusUpdate.setFieldClean(QueryStatus.Field_LASTATTEMPTTIME);
        statusUpdate.setFieldClean(QueryStatus.Field_QUERYID);
        statusUpdate.setFieldClean(QueryStatus.Field_SHARDID);
        // merge remaining data members 
        slaveInfo._queryStatus.merge(statusUpdate);
      } catch (CloneNotSupportedException e) {
      }
      updateRemoteQueryStatus(_associatedRequest);
    }
    else { 
    	LOG.error("Query: " + getQueryId() + " Received Status Update from Unknown Shard:" + statusUpdate.getShardId());
    }
  }
  private static final int QUERY_RETRY_DELAY = 5000;
  private static final int MAX_RETRIES = 5;
  
  private void updateRemoteQueryStatus(final QueryRequest request) throws IOException { 
    
    int completedCount = 0;
    int failedCount    = 0;
    long totalResultCount =0;
    String failureReason = "";
    
    //LOG.info("Update MasterQueryStatus called for Query:" + getQueryId());
    
    for (ShardState shardStatus : _shardStatusMap.values()) { 
      if (shardStatus._queryStatus.getStatus() == QueryStatus.Status.PENDING) {
        
        // if first attempt or time elapsed between last attempt and now exceeds retry delay ... 
        if (shardStatus._queryStatus.getLastAttemptTime() == 0 
            || ((System.currentTimeMillis() - shardStatus._queryStatus.getLastAttemptTime()) >= QUERY_RETRY_DELAY)) {

          //increment attempt count 
          shardStatus.getQueryStatus().setAttempts(shardStatus.getQueryStatus().getAttempts()+1); // increment attempts ... 
          
          if (shardStatus.getQueryStatus().getAttempts() > MAX_RETRIES) { 
            LOG.error("Failed to execute Query:" + getQueryId() + " for Shard:" + shardStatus.getQueryStatus().getShardId());
            shardStatus._queryStatus.setStatus(QueryStatus.Status.ERROR);
            failedCount++;
            continue;
          }
          
          // update attempt time ... 
          shardStatus.getQueryStatus().setLastAttemptTime(System.currentTimeMillis());
          QuerySlaveConnection dispatchPoint = null; 
          
          if (shardStatus.getAssignedNode() == null) {
            String fqnHost = _queryController.getHost().mapShardToHostFQN(this,shardStatus.getQueryStatus().getShardId());
            if (fqnHost != null) {
              dispatchPoint = _queryController.getHost().mapFQNToConnection(fqnHost);
              if (!dispatchPoint.isOnline()) { 
                dispatchPoint = null;
              }
              if (dispatchPoint == null) { 
                LOG.error("Failed to map fqnHost:" + fqnHost + " for shardId:" + shardStatus.getQueryStatus().getShardId() + " and Query:" + getQueryId() + " to valid Connection");
              }
            }
            else { 
              LOG.error("Failed to map shardId:" + shardStatus.getQueryStatus().getShardId() + " for Query:" + getQueryId());
            }
            
            if (dispatchPoint != null) { 
              // ok assign the connection ... 
              shardStatus.setAssignedNode(dispatchPoint);
            }
          }
          if (shardStatus._assignedNode != null) { 
            LOG.info("Sending Remote Query to Slave:" + shardStatus._assignedNode.getFullyQualifiedName() + " for Shard:" + shardStatus.getQueryStatus().getShardId() + " Query:" + getQueryId() );
            try { 
              // flip status to running 
              shardStatus._queryStatus.setStatus(QueryStatus.Status.RUNNING);
              shardStatus._queryStatus.setProgress(0.0f);
              // and displatch the actual query ...
              dispatchQueryToSlave(shardStatus);
            }
            catch (IOException e){
              LOG.error("Remote RPC For Query:" + getQueryId() + " to Slave:" + shardStatus.getAssignedNode().getFullyQualifiedName()
                  + " Failed with Exception:" + CCStringUtils.stringifyException(e));
              
            }
          }

        }
      }
      else if (shardStatus._queryStatus.getStatus() == QueryStatus.Status.FINISHED) {
        
        //LOG.info("Query:" + getQueryId() + " Shard:" + shardStatus._queryStatus.getShardId() + " Marked FINISHED with ResultCount:" + shardStatus._queryStatus.getOptResultCount()); 
        /*
        if (!slaveStatus._logged) { 
          LOG.info("Received Remote Query Finished from Slave:" + slaveStatus.getOnlineState().getFullyQualifiedName() + " Query:" + getQueryId() + 
              " ResultCount:" + slaveStatus._queryStatus.getOptResultCount());
          slaveStatus._logged = true;
        }
        */
          
        totalResultCount += shardStatus._queryStatus.getOptResultCount();
        
        completedCount++;
      }
      else if (shardStatus._queryStatus.getStatus() == QueryStatus.Status.ERROR) {
        if (shardStatus.getQueryStatus().getAttempts() < MAX_RETRIES) { 
          shardStatus._queryStatus.setStatus(QueryStatus.Status.PENDING);
          LOG.info("Marking FAILED Query:" + getQueryId() + " for Retry. Attempts(" +shardStatus.getQueryStatus().getAttempts() + "). Last Error Was:" + shardStatus._queryStatus.getOptErrorReason());
          continue;
        }
        else { 
          failureReason +="\n";
          failureReason += "Failure Reason:" + shardStatus._queryStatus.getOptErrorReason();
          failedCount++;
        }
      }
    }
    // ok update progress ... 
    int shardCount = _shardStatusMap.size();
    float queryPctComplete = (float)completedCount/(float)shardCount;
    float statusPctComplete = .33f * queryPctComplete;
    if (request != null) { 
      request.getQueryStatus().setProgress(statusPctComplete);
    }
    
    // if completed + failed count == slave count ... 
    if ( completedCount + failedCount == _shardStatusMap.size() || (completedCount == 1 && totalResultCount == 1 && isSingleRequestQuery()) ) {
      _associatedRequest = null;
      if (_queryMonitorTimer != null && _queryController != null) { 
        _queryController.getHost().getEventLoop().cancelTimer(_queryMonitorTimer);
        _queryMonitorTimer = null;
      }
      
      //TODO: right now partial failures are considered a complete failure ... 
      if (failedCount != 0) {
        
        LOG.info("Cancellig Query:" + getQueryId() + " because " + failedCount + " Shards Failed");
        
        //if (completedCount == 0) { 
          // udpate query status 
          _queryStatus.setStatus(QueryStatus.Status.ERROR);
          _queryStatus.setOptErrorReason(failureReason);
          _completionCallback.queryFailed(this,failureReason);
        // }
      }
      
      // if (completedCount != 0)
      else
      {
        LOG.info("Query:" + getQueryId() + " Successfully Completed with Result Count:" + totalResultCount);
        // udpate query status 
        _queryStatus.setStatus(QueryStatus.Status.FINISHED);
        _queryStatus.setOptResultCount(totalResultCount);
        _completionCallback.queryComplete(this,totalResultCount);
      }
      
      // clear state ... 
      _shardStatusMap.clear();
    }
  }
  
 
  public long getQueryId() { return getCommonQueryInfo() .getQueryId(); }
  public void setQueryId(long queryId) { 
    
    getCommonQueryInfo().setQueryId(queryId);
    _queryStatus.setQueryId(queryId);
  }
  
  /** cancel a slave query **/
  public void cancelSlaveQuery() { 
    synchronized (this) {
      _cancelQuery = true;
    }
    if (_queryThread != null) { 
      try {
        _queryThread.join();
      } catch (InterruptedException e) {
      }
    }
  }
  
  public void waitOnQuery() { 
    if (_queryThread != null) { 
      try {
        _queryThread.join();
      } catch (InterruptedException e) {
      }
    }
  }

  final void dispatchQueryToSlave(final ShardState shardStatus)throws IOException {
    // save name 
    final String targetHostFQN = shardStatus.getAssignedNode().getFullyQualifiedName(); 
    
    //LOG.info("Dispatching Query:" + getQueryId() + " to Slave:" + slave.getOnlineState().getFullyQualifiedName());
    shardStatus.getAssignedNode().getRemoteStub().doQuery(shardStatus.getQueryInfo(), new OutgoingMessageContext.Callback<RemoteQueryInfo, QueryStatus>() {
      @Override
      public void requestComplete(OutgoingMessageContext<RemoteQueryInfo, QueryStatus> request) {
        
        if (request.getStatus() != OutgoingMessageContext.Status.Success) {
          LOG.error("Query:" + getQueryId() + " To Slave:" + targetHostFQN  + " Failed with RPC Error:" + request.getStatus());
          shardStatus._queryStatus.setStatus(QueryStatus.Status.PENDING);
        }
        else { 
        	LOG.info("Query:" + getQueryId() + " To Slave:" + targetHostFQN  + " returned Status:" + request.getOutput().getStatus());
        	try {
        	  request.getOutput().setFieldClean(QueryStatus.Field_ATTEMPTS);
        	  request.getOutput().setFieldClean(QueryStatus.Field_LASTATTEMPTTIME);
        	  request.getOutput().setFieldClean(QueryStatus.Field_QUERYID);
        	  request.getOutput().setFieldClean(QueryStatus.Field_SHARDID);
        	  
	          shardStatus.getQueryStatus().merge(request.getOutput());
          } catch (CloneNotSupportedException e) {
          	// NOOP
          }
        }
      } 
    });
    //LOG.info("Done Dispatching Query:" + getQueryId() + " to Slave:" + slave.getOnlineState().getFullyQualifiedName());
  }
  
  Pair<Long,Long> _cachedCanonicalId = null;
  
  /** get a unique canonical id that identifies this query instance **/
  public Pair<Long,Long> getCanonicalQueryId() {
    if (_cachedCanonicalId == null) { 
      String domainId = getQueryDomainId();
      String parameters = getUniqueQueryParameters();
      _cachedCanonicalId = new Pair<Long,Long>(FPGenerator.std64.fp(domainId),FPGenerator.std64.fp(parameters));
    }
    return _cachedCanonicalId;
  }

  /** get a unique string that represents the specifc query type **/
  public  abstract  String getQueryDomainId();
  
  /** get the unique query parameters associated with this query **/
  public abstract String getUniqueQueryParameters();
    
  
  //protected long executeAggregationQuery(final FileSystem fileSystem,final Configuration conf,final EventLoop eventLoop,final File tempFirDir,QueryRequest<DataType,ResultKeyType,ResultValueType> requestObject)throws IOException { return 0; }

  public static final String MERGED_RESULTS_SUFFIX = "MERGED_RESULTS";
  
  /** are cached results available (runs on master only)**/
  protected boolean cachedResultsAvailable(FileSystem remoteFS,Configuration conf,ClientQueryInfo clientQueryInfo) throws IOException {
        
    Path outputFileName = new Path(getHDFSQueryResultsPath(),MERGED_RESULTS_SUFFIX);
    
    //LOG.info("Cached Results Available called for Query:" + getQueryId() + ". Checking Path:" + outputFileName);
    boolean result = remoteFS.exists(outputFileName);
    //LOG.info("Cached Results Available called for Query:" + getQueryId() + ". returning:" + result);
    return result;
  }

  /** get cached results (runs on master only)**/
  protected void executeCacheQuery(FileSystem remoteFS, Configuration conf,
      EventLoop eventLoop, QueryController queryController,
      QueryRequest<DataType, ResultKeyType, ResultValueType> theClientRequest,
      QueryCompletionCallback<DataType, ResultKeyType, ResultValueType> callback)
      throws IOException {
    
    
    LOG.info("getCachedResults called for Query:" + getQueryId());
      
    Path outputFileName = new Path(getHDFSQueryResultsPath(),MERGED_RESULTS_SUFFIX);
      
    //LOG.info("Initializing index reader for outputFile:" + outputFileName);
    Path indexFileName = QueryResultFileIndex.getIndexNameFromBaseName(outputFileName);
      //LOG.info("Index FileName is:" + indexFileName);
      
    QueryResultFileIndex<ResultKeyType, ResultValueType> index = new QueryResultFileIndex<ResultKeyType, ResultValueType>(remoteFS,indexFileName,getKeyClass(),getValueClass());
      
    QueryResult<ResultKeyType, ResultValueType> resultOut = new QueryResult<ResultKeyType, ResultValueType>(); 
      
    LOG.info("getCachedResults called for Query:" + getQueryId() +" Calling ReadPaginationResults");
    index.readPaginatedResults(remoteFS, conf,
        theClientRequest.getClientQueryInfo().getSortOrder(),
        theClientRequest.getClientQueryInfo().getPaginationOffset(),
        theClientRequest.getClientQueryInfo().getPageSize(),
        resultOut);
    
    LOG.info("getCachedResults called for Query:" + getQueryId() +". Initiating getCachedResults Callback");
    callback.queryComplete(theClientRequest,resultOut);
  }      
  
  /**
   * an advanced overload ... basically return the spillwriter to use when creating the aggregated merge result
   *  
   * @param remoteFileSystem
   * @param conf
   * @param mergeResultsPath
   * @param indexWriter
   * @return
   * @throws IOException
   */
  protected SpillWriter<ResultKeyType, ResultValueType> createAggregatedResultSpillWriter(
      FileSystem remoteFileSystem, 
      Configuration conf,
      Path mergeResultsPath,
      QueryResultFileIndex.PositionBasedIndexWriter indexWriter)throws IOException { 
   
    return new SequenceFileSpillWriter<ResultKeyType, ResultValueType>(remoteFileSystem,conf,mergeResultsPath,getKeyClass(),getValueClass(),
        new QueryResultFileIndex.PositionBasedIndexWriter(remoteFileSystem,QueryResultFileIndex.getIndexNameFromBaseName(mergeResultsPath))
        ,null,(short)2);
  }
      
  private long _aggregationCompletionCount = 0L;
  
  /** run the local porition of this query (runs on master)**/
  protected long executeAggregationQuery(FileSystem remoteFileSystem, Configuration conf,
      EventLoop eventLoop, File tempFirDir,
      QueryRequest<DataType, ResultKeyType, ResultValueType> requestObject,final QueryProgressCallback progressCallback)
      throws IOException {

    Path mergeResultsPath = new Path(getHDFSQueryResultsPath(),MERGED_RESULTS_SUFFIX);
    
    try { 
      LOG.info("Execute Local called for Query:" + getQueryId() +" MergeResultsPath is:" + mergeResultsPath);
      
      //LOG.info("Executing LocalQuery - checking if MergedFile:" + mergeResultsPath + " Exists");
      // if source merged results path does not exist ... 
      if (!remoteFileSystem.exists(mergeResultsPath)) {
        LOG.info("Execute Local for Query:" + getQueryId() +" Source MergeFile:" + mergeResultsPath + " Not Found. Checking for parts files");
        // collect parts ...
        Vector<Path> parts = new Vector<Path>();
        
        FileStatus fileStatusArray[] = remoteFileSystem.globStatus(new Path(getHDFSQueryResultsPath(),"part-*"));
        
        if(fileStatusArray.length == 0) {
          LOG.error("Execute Local for Query:" + getQueryId() +" FAILED. No Parts Files Found!");
          throw new IOException("Remote Component Part Files Not Found");
        }
  
        for (FileStatus part : fileStatusArray) { 
          //LOG.info("Found Part:"+ part);
          parts.add(part.getPath());
        }
        
        LOG.info("Execute Local for Query:" + getQueryId() +" Initializing Merger");
        SpillWriter<ResultKeyType, ResultValueType> mergedFileSpillWriter 
          = createAggregatedResultSpillWriter(remoteFileSystem,conf,mergeResultsPath,new QueryResultFileIndex.PositionBasedIndexWriter(remoteFileSystem,QueryResultFileIndex.getIndexNameFromBaseName(mergeResultsPath)));
        try { 
            SequenceFileMerger<ResultKeyType, ResultValueType> merger 
              = new SequenceFileMerger<ResultKeyType, ResultValueType>(
                  remoteFileSystem,
                  conf,
                  parts,
                  mergedFileSpillWriter,
                  getKeyClass(),
                  getValueClass(),
                  allocateRawComparator());
            
            try { 
              LOG.info("Execute Local for Query:" + getQueryId() +" Running Merger");
              merger.mergeAndSpill(new Reporter() {
                
                @Override
                public void progress() {
                }
                
                @Override
                public void setStatus(String status) {
                }
                
                @Override
                public void incrCounter(String group, String counter, long amount) {
                }
                
                @Override
                public void incrCounter(Enum<?> key, long amount) {
                  if (key == SequenceFileMerger.Counters.PCT_COMPLETED) {
                    _aggregationCompletionCount += amount;
                    if (progressCallback != null) { 
                      float pctComplete = (float)_aggregationCompletionCount / (float)100;
                      progressCallback.updateProgress(Query.this, pctComplete);
                    }
                  }
                }
                
                @Override
                public InputSplit getInputSplit() throws UnsupportedOperationException {
                  return null;
                }
                
                @Override
                public Counter getCounter(String group, String name) {
                  return null;
                }
                
                @Override
                public Counter getCounter(Enum<?> name) {
                  return null;
                }
              });
              LOG.info("Execute Local for Query:" + getQueryId() +" Merge Successfull.. Deleting Merge Inputs");
              for (Path inputPath : parts) { 
                remoteFileSystem.delete(inputPath,false);
              }
            }
            catch (IOException e){ 
              LOG.error("Execute Local for Query:" + getQueryId() +" Merge Failed with Exception:" + CCStringUtils.stringifyException(e));
              throw e;
            }
            finally { 
              LOG.info("** CLOSING MERGER");
              merger.close();
            }
        }
        finally {
          LOG.info("** FLUSHING SPILLWRITER");
          mergedFileSpillWriter.close();
        }
      }
      
      Path indexFileName = QueryResultFileIndex.getIndexNameFromBaseName(mergeResultsPath);
      
      //LOG.info("Allocating SequenceFileIndex object for DomainListQuery Id:" + getQueryId() + " with Path:" + queryResultsPath);
      QueryResultFileIndex<ResultKeyType, ResultValueType> indexFile = new QueryResultFileIndex<ResultKeyType, ResultValueType>(remoteFileSystem,indexFileName,getKeyClass(),getValueClass());
      //LOG.info("SequenceFileIndex object for DomainListQuery Id:" + getQueryId() + " with Path:" + queryResultsPath + " returned record count:" + indexFile.getRecordCount());
      
      return indexFile.getRecordCount();
    }
    catch (Exception e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      throw new IOException(e);
    }
  }  
  
  /** does the specified client query require a remote dispatch (runs on master only)
   * 	returns TRUE if query requires execution on one or more shards. The shardsToRunOn is populated
   *  on return with the shards on which this query should execute on.
   * **/
  public  boolean requiresShardedQuery(FileSystem remoteFS,Configuration conf,QueryController controller,QueryRequest<DataType,ResultKeyType,ResultValueType> theClientRequest,int shardCount,ArrayList<QueryInputSplit> splitsOut) throws IOException { 
    // create a set representing the collection of parts required to complete this query ... 
    Set<String> requiredParts = new HashSet<String>();
    
    for (int i=0;i<shardCount;++i) {  
      requiredParts.add(getPartNameForSlave(i));
    }
    
    // now iterate parts available on hdfs ... 
    Path remoteQueryPath = getHDFSQueryResultsPath();
    //LOG.info("Results Path is:" + remoteQueryPath);
    
    FileStatus availableParts[] = remoteFS.globStatus(new Path(remoteQueryPath,"part-*"));
    
    for (FileStatus part : availableParts) { 
      //LOG.info("Found Path:" + part.getPath());
      requiredParts.remove(part.getPath().getName());
    }
    
    // now check to see if all parts are available 
    if (requiredParts.size() != 0) {
      // ok now create shards ... 
      for (String part: requiredParts) { 
        QueryInputSplit split = new QueryInputSplit();
        
        // set shard id ... 
        split.setShardId(Integer.parseInt(part.substring("part-".length())));
        //LOG.info("Required remote part:" + part + " NOT available yet for shard:" + split.getShardId());
        // add it to required shards list ... 
        splitsOut.add(split);
      }
      return true;
    }
    else { 
      LOG.info("All parts required for query available.");
      return false;
    }
  }
  
  
  /** is high priority query (can be run on master or slave)
   * 	return TRUE if this query should be dispatched immediately, instead of being queued.
   */
  public 	boolean isHighPriorityQuery() { return false; }
  
  /** run the remote part of this query  (runs remote code on slave) **/
  protected abstract long executeShardedQuery(
      final FileSystem fileSystem,
      final Configuration conf,
      final EventLoop eventLoop,
      final QuerySlaveServer slaveInterface,
      final File tempFirDir,
      final Path hdfsTempDir,
      final MergeSortSpillWriter<ResultKeyType,ResultValueType> spillWriter,
      final QueryProgressCallback<DataType,ResultKeyType,ResultValueType> progressCallback)throws IOException;
  
  /** remote dispatch complete notification (runs on master)**/
  public    void shardedQueryComplete(FileSystem fileSystem,Configuration conf,QueryRequest<DataType,ResultKeyType,ResultValueType> theClientRequest,long resultCount)throws IOException {} 
  

  /** remote dispatch failed notification (runs on master)**/
  public    void shardedQueryFailed(FileSystem fileSystem) {} 
  
  
  /** is single result query **/
  //TODO: AHAD - ELEMINATE THIS 
  public  boolean  isSingleRequestQuery() { return false; }
  
  public abstract Class<ResultKeyType> getKeyClass();
  public abstract Class<ResultValueType> getValueClass();
  public abstract RawKeyValueComparator<ResultKeyType,ResultValueType> allocateRawComparator();
  
  
  //////////////////////////////////////////////////////////////////////////////////////////////////
  // internal methods ...   
  //////////////////////////////////////////////////////////////////////////////////////////////////
  protected synchronized static String getPartNameForSlave(int slaveIndex) { 
    return "part-" + NUMBER_FORMAT.format(slaveIndex);
  }
  protected Path getHDFSQueryResultsPath() { return new Path(getCommonQueryInfo().getQueryResultPath()); }
  protected Path getHDFSQueryTempPath() { return new Path(getCommonQueryInfo().getQueryTempPath()); }
  protected Path getHDFSQueryAttemptFilePathForShard(int shardIndex) {
    return new Path(getHDFSQueryTempPath(),getPartNameForSlave(shardIndex)+"-"+System.currentTimeMillis());
  }
}
