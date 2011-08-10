package org.commoncrawl.query;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.Timer;
import org.commoncrawl.query.BaseConfig;
import org.commoncrawl.query.QueryServerSlave;
import org.commoncrawl.query.SlaveStatus;
import org.commoncrawl.rpc.OutgoingMessageContext;
import org.commoncrawl.rpc.RPCChannel;
import org.commoncrawl.rpc.EmptyStruct;
import org.commoncrawl.rpc.OutgoingMessageContext.Callback;
import org.commoncrawl.rpc.MessageData.Status;
import org.commoncrawl.util.shared.CCStringUtils;

///////////////////////////////////////////////////////
/* Online Crawler State Object */
///////////////////////////////////////////////////////

/** helper object used to encapsulate an online crawler's state information **/
public class QuerySlaveConnection implements RPCChannel.ConnectionCallback { 
  
  private static final int  HEARTBEAT_TIMER_INTERVAL = 50;
  private static final Log LOG = LogFactory.getLog(QuerySlaveConnection.class);  
  private String            _hostName;
  private int               _hostRPCPort;
  private InetSocketAddress _hostAddress;
  private long              _lastUpdateTime = -1;
  private QueryController   _controller;
  private Timer             _heartbeatTimer = null;
  private boolean           _ignoreHeartbeats = false;
  private boolean           _online = false;
  
  private RPCChannel _channel;
  private QueryServerSlave.AsyncStub _slaveService;

  public QuerySlaveConnection(QueryController master,String hostName,int rpcPort){ 
    
    _controller = master;
    _hostName   = hostName;
    _hostRPCPort   = rpcPort;
    InetAddress slaveAddress = null;
    try {
      LOG.info("Resolving Slave Address for Slave:" + hostName);
      slaveAddress = InetAddress.getByName(hostName);
      LOG.info("Resolving Slave Address for Slave:" + hostName + " to:" + slaveAddress.getHostAddress());
    } catch (UnknownHostException e) {
      LOG.error("Unable to Resolve Slave HostName:" + hostName + " Exception:" + CCStringUtils.stringifyException(e));
      throw new RuntimeException("Unable to Resolve Slave HostName:" + hostName + " Exception:" + CCStringUtils.stringifyException(e));
    }
    _hostAddress = new InetSocketAddress(slaveAddress.getHostAddress(),rpcPort);
    
    if (_hostAddress == null) { 
      throw new RuntimeException("Invalid HostName String in Query Slave Registration: " + _hostName);
    }
    else { 
      LOG.info("Host Address for Slave:" + hostName +" is:" + _hostAddress);
    }
  }

  public void connect() throws IOException { 
    LOG.info("Opening Channel to Host:" + getFullyQualifiedName());
    // initialize channel ... 
    _channel = new RPCChannel(_controller.getHost().getEventLoop(),null,_controller.getHost().getOutgoingInterface(),_hostAddress,this);
    _channel.open();
    _slaveService = new QueryServerSlave.AsyncStub(_channel,null);
  }
  
  public void shutdown() {
    
    killHeartbeatTimer();
    
    if (_channel != null) { 
      try {
        _channel.close();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
  }

  public String       getHostName() { return _hostName; } 
  public int          getPort() { return _hostRPCPort; }
  public String       getFullyQualifiedName() { return getHostName() + ":" + getPort(); }

  public long         getLastUpdateTime() { return _lastUpdateTime; }
  
  public boolean isOnline() { return _online; }
  
  public QueryServerSlave.AsyncStub getRemoteStub() { return _slaveService; } 
  
  void enableHeartbeats() { _ignoreHeartbeats = false; }
  void disableHeartbeats() { _ignoreHeartbeats = true; }
  boolean areHeartbeatsDisabled() { return _ignoreHeartbeats; }
  
  public void OutgoingChannelConnected(RPCChannel channel) {
    LOG.info("Connected to Query Slave:" + getFullyQualifiedName());
    slaveOnline();
  }

  public boolean OutgoingChannelDisconnected(RPCChannel channel) {
    LOG.info("Disconnect detected for Slave : "+ getFullyQualifiedName());
    slaveOffline();
    return false;
  }
  
  private void slaveOnline() {
    
    try { 
      // initialize the slave ... 
      _slaveService.initializeQuerySlave(_controller.getBaseConfigForSlave(getFullyQualifiedName()), new Callback<BaseConfig,SlaveStatus> () {
  
          @Override
          public void requestComplete(OutgoingMessageContext<BaseConfig, SlaveStatus> request) {
            if (request.getStatus() != Status.Success) { 
              LOG.error("resetState failed on Slave:" + getFullyQualifiedName());
              slaveOffline();
            }
            else {
              _online = true;
              
              // notify master of status change ...
              updateSlaveStatus(request.getOutput());
              // start the heartbeat timer ... 
              startHeartbeatTimer();
            }
          }
      });
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      slaveOffline();
    }
  }
  
  private void slaveOffline() { 
    
    _online = false;
    
    // kill heartbeats... 
    killHeartbeatTimer();
    // inform master ... 
    _controller.slaveStatusChanged(getFullyQualifiedName(),null);

    // reconnect channel
    if (_channel != null) { 
      try {
        _channel.reconnect();
      } catch (IOException e) {
        LOG.error(CCStringUtils.stringifyException(e));
      }
    }
  }
  
  
  private void updateSlaveStatus(SlaveStatus status) { 
    _lastUpdateTime = System.currentTimeMillis();
    // and inform master ... 
    _controller.slaveStatusChanged(getFullyQualifiedName(),status);
  }
  
  
  private void startHeartbeatTimer() { 
    _heartbeatTimer = new Timer(HEARTBEAT_TIMER_INTERVAL,false,new Timer.Callback() {

      @Override
      public void timerFired(final Timer timer) {

        //LOG.info("Heartbeat Timer Fired. Sending heartbeat message to slave:" + getFullyQualifiedName());
        try { 
          _slaveService.heartbeatQuerySlave(new Callback<EmptyStruct,SlaveStatus>() {

            public void requestComplete(OutgoingMessageContext<EmptyStruct, SlaveStatus> request) {
              
              //LOG.info("Received Heartbeat message Response from Slave:"+ getFullyQualifiedName());
              
              boolean forceDisconnect = false;
              
              if (request.getStatus() == Status.Success) { 

                if (!areHeartbeatsDisabled()) {
                  if (request.getOutput().getQueryStatus().size() != 0) { 
                    //LOG.info("Received non-zero QueryStatus list from slave:" + getFullyQualifiedName());
                  }
                  //LOG.info("updating SlaveStatus from heartbeat response for Slave:"+ getFullyQualifiedName());
                  // update slave status ...
                  updateSlaveStatus(request.getOutput());
                }
                else { 
                  // LOG.info("heartbeats are disabled. Skipping response for Slave:"+ getFullyQualifiedName());
                }

                // need to SET timer because we are not in timerFired context anymore 
                _controller.getHost().getEventLoop().setTimer(timer);
                
              }
              else { 
                // LOG.error("Heartbeat request to slave: " + getFullyQualifiedName() +" failed with Status: " + request.getStatus().toString());
                forceDisconnect = true;
              }
              
              if (forceDisconnect) { 
                slaveOffline();
              }
            } 
            
          });
        }
        catch (IOException e ){ 
          slaveOffline();
          LOG.error(CCStringUtils.stringifyException(e));
        }
      } 
      
    });
    
    _controller.getHost().getEventLoop().setTimer(_heartbeatTimer);
  }
  
  private void killHeartbeatTimer() { 
    if (_heartbeatTimer != null) { 
      _controller.getHost().getEventLoop().cancelTimer(_heartbeatTimer);
      _heartbeatTimer = null;
    }
  }
  
  
}
