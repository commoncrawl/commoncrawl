package org.commoncrawl.query;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import org.commoncrawl.async.EventLoop;
import org.commoncrawl.query.BaseConfig;
import org.commoncrawl.query.QueryInputSplit;
import org.commoncrawl.query.SlaveStatus;

public interface QueryControllerHost {

  public EventLoop getEventLoop();
  
  public InetSocketAddress getOutgoingInterface();
  
  public int    getShardCountForQuery(Query targetQuery);
  
  public String mapShardToHostFQN(Query targetQuery,int shardId);
  
  public QuerySlaveConnection mapFQNToConnection(String hostFQN);
  

}
