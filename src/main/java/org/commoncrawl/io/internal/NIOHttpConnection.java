/**
 * Copyright 2008 - CommonCrawl Foundation
 * 
 * CommonCrawl licenses this file to you under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.commoncrawl.io.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.FutureTask;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.io.internal.NIOBufferList.CRLFReadState;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.util.shared.BandwidthUtils;
import org.commoncrawl.util.shared.CustomLogger;
import org.commoncrawl.util.shared.IPAddressUtils;
import org.junit.Test;


/**
 * 
 * @author rana
 * 
 * NIOHttpConnection - Async HTTP Connection class
 * 
*/

public final class NIOHttpConnection implements NIOClientSocketListener , NIODNSQueryClient  {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(NIOHttpConnection.class);
  
  /** Listener interface used to propagate Connection Status */
  public static interface Listener { 
    
    /** 
     * Called whenever the source connection changes state
     * @param theConnection - source connection triggering the callback
     * @param oldState - old connection state 
     * @param state - new connection state 
     */
    void HttpConnectionStateChanged(NIOHttpConnection theConnection,State oldState,State state);

    /**
     * Called whenever there is HTTP Content available to be read from the content buffer 
     * @param contentBuffer - the HTTPConnection's content buffer list ...
     */
    void HttpContentAvailable(NIOHttpConnection theConnection,NIOBufferList contentBuffer);
  };
  
  public static interface DataSource { 
    /** add any new content into data buffer 
     *   and return true on EOF 
     *   
     * @param dataBuffer
     * @return
     */
    boolean read(NIOBufferList dataBuffer) throws IOException;
  }
  
  enum ChunkState { 
    /**
     * State to indicate that next field should be :-
     *  chunk-size [ chunk-extension ] CRLF
     */
    STATE_AWAITING_CHUNK_HEADER,

    /**
     * State to indicate that we are currently reading the chunk-data.
     */
    STATE_READING_CHUNK ,

    /**
     * Indicates that a chunk has been completely read and the next 
     * fields to be examine should be CRLF
     */
    STATE_AWAITING_CHUNK_EOL,

    /**
     * Indicates that all chunks have been read and the next field
     * should be optional trailers or an indication that the chunked
     * stream is complete.
     */
    STATE_AWAITING_TRAILERS,

    /**
     * State to indicate that the chunked stream is complete and
     * no further bytes should be read from the underlying stream.
     */
    STATE_DONE
    
  }
  
  
  /** MAX HTTP HEADER SIZE */
  private static final int     HTTP_HEADER_SIZE_MAX = 1 << 14;
  /** MAX CHUNK LINE SIZE **/
  private static final int     CHUNK_LINE_MAX = 1024;
  
  /** HTTP REQUEST TIMEOUT - IN MILLISECONDS */
  private static final int     DNS_TIMEOUT_DEFAULT = 40000;
  private static final int     TIMEOUT_DEFAULT = 30000;
  private static final int     UPLOAD_DOWNLOAD_TIMEOUT_DEFAULT = 30000;
  private static final int     MIN_DNS_CACHE_TIME = 60 * 60 * 1000;  
  
  /** charset for UTF-8 conversion */
  private static final Charset _utf8Charset = Charset.forName("UTF8");
  /** the set of outgoing HTTP Headers */
  private final NIOHttpHeaders _requestHeaders  = new NIOHttpHeaders();
  /** populate default header items (default:true) **/
  private boolean _populateDefaultHeaderItems = true;
  /** the set of incoming HTTP Headers */
  private final NIOHttpHeaders _responseHeaders = new NIOHttpHeaders();
  /** found status line **/
  private boolean _foundStatusLine = false;
  /** header reader state **/
  private boolean _lastCharWasLF = false;
  /** header reader state **/
  private byte    _lastChar = 0;
  /** the output buffer */
  private final NIOBufferList  _outBuf = new NIOBufferList();
  /** the input buffer */
  private NIOBufferList      _inBuf           = new NIOBufferList();
  /** the header accumulation buffer **/ 
  private ByteArrayOutputStream _incomingAccumulationBuffer = null;
    
  /** the underlying Socket object to be used for request */
  private NIOClientSocket            _socket      = null;
  /** the underlying selector used to poll sockets */
  private NIOSocketSelector    _selector    = null;
  /** resolver to use for DNS resolution */
  private NIODNSResolver       _resolver    = null;
  /** cumilative bytes read **/
  private static long  _cumilativeRead = 0;
  /** cumilative byte written */
  private static long  _cumilativeWritten = 0;  
  
  /** Statistic: total bytes written */
  private int                 _totalWritten    = 0;
  /** Statistic: total bytes read */
  private int                 _totalRead       = 0;
  /** Connection closed indicator */
  private boolean              _closed = false;
  /** the TARGET URL */
  private URL                  _url = null;
  /** resolved address **/
  private  InetAddress                  _resolvedAddress = null;
  /** source address **/
  private   InetSocketAddress            _sourceIP = null;
  /** resolved address ttl **/
  private long                             _resolvedAddressTTL = -1;
  /** resolved cname **/
  private String                            _resolvedCName;
  /** the Status Listener */
  private Listener _listener = null;
  /** optional data source (for uploads) **/
  private DataSource _dataSource = null;
  /** the Context Object **/
  private Object	_context = null;
  /** content length - as retrieved from HTTP Headers */
  private int                  _contentLength = -1;
  /** downloaded content length - amount we have downloaded so far **/
  private int                  _downloadedContentLength = 0;
  /** content max - used to limit downloaded content **/
  private int                 _downloadMax = -1;
  /** truncated flag - indicates content was truncated to accommodate download max limit **/
  private boolean        _contentTruncated = false;
  /** transfer encoding is chunked**/
  private boolean         _chunked = false;
  /** proxy server host **/
  private InetSocketAddress     _proxyServer;
  /** cookie logger (optional) */
  private static CustomLogger _cookieLogger;
  
  /** chunk stream state **/
  ChunkState                 _chunkState = ChunkState.STATE_AWAITING_CHUNK_HEADER;
  /** chunk size **/
  int  _chunkSize = 0;
  /** chunk pos **/
  int _chunkPos = 0;
  /** chunk line reader state **/
  CRLFReadState _chunkCRLFReadState = CRLFReadState.NONE;
  /** chunk line buffer **/
  StringBuffer      _chunkLineBuffer = null;
  /** chunk content buffer **/
  NIOBufferList                    _chunkContentBuffer = null;
  /** total download length  (Headers + Content) */
  private int                  _downloadLength = 0;
  /** open time - absolute time at which this socket was opened **/
  private long               _openTime = -1;
  /** Statistic: time it took for DNS resolution (MS)*/  
  private int                  _resolveTime = 0;
  /** Statistic: time it took to connect (after resolution) (MS)*/
  private int                  _connectTime = 0;
  /** Statistic: time it took to upload headers (after connection) (MS)*/
  private int                  _uploadTime  = 0;
  /** Statistic: time it took to download headers + content (after sending headers ) (MS)*/
  private int                  _downloadTime = 0;
  /** id for tracking purposes **/
  private int                  _id = 0;
  
  /** internal phase start time */
  private long                 _phaseStartTime;
  /** last time this socket received some data **/
  private long                 _lastReadOrWriteTime = -1;
  
  /** modifiable timeout value  */
  private int           _dnsTimeout = DNS_TIMEOUT_DEFAULT;
  private int           _connectTimeout = TIMEOUT_DEFAULT;
  private int           _uploadDownloadTimeout = UPLOAD_DOWNLOAD_TIMEOUT_DEFAULT;
  
  /** HTTP METHOD */
  private String               _method                = "GET";
  /** HTTP VERSION STRING */
  private String               _httpVersionString     = "HTTP/1.1";
  /** DEFAULT USER AGENT STRING */
  private static String     _defaultUserAgentString = "Mozilla/5.0 (compatible; NIOHttpConnection/1.0;)"; 
  
  /** State Machine States*/
  public enum State { IDLE, AWAITING_RESOLUTION,AWAITING_CONNECT, SENDING_REQUEST, RECEIVING_HEADERS,PARSING_HEADERS,RECEIVING_CONTENT, DONE, ERROR };
  /** State Machine State */
  private State                _state                 = State.IDLE;
  /** timeout state **/
  private State                _timeoutState      = State.IDLE;
  /** Error Type Enum **/
  public enum ErrorType { UNKNOWN,RESOLVER_FAILURE,DNS_FAILURE,IOEXCEPTION,TIMEOUT } 
  /** Error Type Variable **/
  private ErrorType _errorType = ErrorType.UNKNOWN;
  /** Error Desc **/
  private String 				_errorDesc;
  /** Last Exception - for tracking errors */
  private Exception            _lastException         = null;
 /** Rate Limit Support **/
  private BandwidthUtils.RateLimiter _uploadRateLimiter;
  /** optional cookie store **/
  private NIOHttpCookieStore _cookieStore;
  
  /** internal constructor - for test purposes **/
  private NIOHttpConnection() {
    
  }
  
  /**
   * 
   * Constructor 
   * 
   * @param theURL   - the target URL 
   * @param selector - shared socket selector object 
   * @param resolver - shared resolver object  
   * 
   * */
  public NIOHttpConnection(URL theURL,NIOSocketSelector selector,NIODNSResolver resolver,NIOHttpCookieStore cookieStore)throws IOException {
    _url = theURL;
    _socket = NIOSocketFactory.createClientSocket(theURL,null,this);
    _sourceIP = _socket.getLocalSocketAddress(); 
    _selector = selector;
    _resolver = resolver;
    _cookieStore = cookieStore;
  }

  /**
   * 
   * Constructor 
   * 
   * @param theURL   - the target URL
   * @param theLocalAddress   - the local ip address to bind to
   * @param selector - shared socket selector object 
   * @param resolver - shared resolver object  
   * 
   * */
  public NIOHttpConnection(URL theURL,InetSocketAddress localBindAddress,NIOSocketSelector selector,NIODNSResolver resolver,NIOHttpCookieStore cookieStore)throws IOException {
    _url = theURL;
    _sourceIP =  localBindAddress;
    _socket = NIOSocketFactory.createClientSocket(theURL,localBindAddress,this);
    _selector = selector;
    _resolver = resolver;
    _cookieStore = cookieStore;
  }
  
  /** set cookie logger **/ 
  public static void setCookieLogger(CustomLogger logger) { _cookieLogger = logger; } 
  /** set proxy server **/
  public void setProxyServer(InetSocketAddress proxyServerAddress) { _proxyServer = proxyServerAddress; }
  public InetSocketAddress getProxyServer() { return _proxyServer; }
  
  /** cumilative bytes read **/
  public static long getCumilativeBytesRead() { return _cumilativeRead; } 
  /** cumilative byte written */
  public static long getCumilativeBytesWritten() { return _cumilativeWritten; }  
  
  /** set the HTTP Method - defaults to GET **/
  public void setMethod(String method)                 { _method = method; }
  /** set the HTTP Version string - defaults to 1.1 **/
  public void setHttpVersionString(String httpVersion) { _httpVersionString= httpVersion; }
  
  /** set the user agent string - defaults to NIOHttpConnection/1.0 **/
  public static void setDefaultUserAgentString(String userAgentString) { _defaultUserAgentString = userAgentString; }
  
  /** set / get event listener **/
  public  final Listener  getListener() { return _listener; }
  public  final void      setListener(Listener listener) { _listener = listener; }
  
  /** set / get data source **/
  public  final void   setDataSource(DataSource source) { _dataSource = source; }
  public  final DataSource getDataSource() { return _dataSource; }
  
  /** set / get the context object **/
  public  final void 	 setContext(Object contextObj) { _context = contextObj; }
  public  final Object  getContext() { return _context; }
  
  /** get local address **/
  public InetSocketAddress getLocalAddress() { return _sourceIP; }
  
  /** set default request header values (if not present) **/
  public final void setPopulateDefaultHeaderItems(boolean value) { _populateDefaultHeaderItems = value; }
  
  /** get the connection state **/
  public final State     getState() { return _state; }
  
  /** in which state did the connection timeout ? **/
  public final State     getTimeoutState() { return _timeoutState; }
  
  private final void     setState(State newState,Exception e) { 
    
    long currentTime = System.currentTimeMillis();
        
    State oldState = _state;
    
    _state = newState;
    if (e != null)
      _lastException = e;
    if (_listener != null) { 
      _listener.HttpConnectionStateChanged(this,oldState,newState);
    }

    if (newState != State.ERROR) {
      
      switch (oldState) { 
        case AWAITING_RESOLUTION: _resolveTime = (int)(currentTime - _phaseStartTime);break;
        case AWAITING_CONNECT:     _connectTime = (int)(currentTime - _phaseStartTime);break;
        case SENDING_REQUEST:        _uploadTime = (int)(currentTime - _phaseStartTime);break;
        case RECEIVING_CONTENT:    _downloadTime = (int)(currentTime - _phaseStartTime);break;
      }
      

      switch (newState) { 
        case AWAITING_RESOLUTION: 
        case AWAITING_CONNECT:     
        case SENDING_REQUEST:        
        case RECEIVING_HEADERS: { 
          if (oldState != newState)
            _phaseStartTime = currentTime;
        }
        break;
      
      }
      
      if (newState.ordinal() >= State.SENDING_REQUEST.ordinal()) { 
        _lastReadOrWriteTime = currentTime;
      }
    }
    
    if (newState == State.ERROR) { 
      // System.out.println("ERROR");
    }
    if (newState == State.DONE || newState == State.ERROR) { 
      close();
    }
  }
  
  /** get / set error type **/
  void setErrorType(ErrorType errorType) { _errorType = errorType; }
  public ErrorType getErrorType() { return _errorType; }

  /** get / set error description **/
  public String        getErrorDesc() { return _errorDesc; }
  void setErrorDesc(String errorDesc) { _errorDesc = errorDesc; } 
  
  /** get the http response code **/
  public final int getHttpResponseCode() { 
    return _responseHeaders.getHttpResponseCode();
  }
  /** does the response code indicate a redirect **/
  public boolean isRedirectResponse() { 
    switch (getHttpResponseCode()) { 
      case 300:
      // permanent
      case 301: 
      // use proxy ... 
      case 305:
      // temporary 
      case 302: 
      // redirect after post 
      case 303: 
      // temporary redirect 
      case 307: { 
        return true;
      }
    }
    return false;
  }
  
  /** get redirect location **/
  public String getRedirectLocation() { 
    int key = _responseHeaders.getKey("Location");
    if (key == -1) { 
      // attempt lowercase version ...  
      key = _responseHeaders.getKey("location");
    }
    if (key != -1) { 
      return _responseHeaders.getValue(key);
    }
    return null;
  }
  
  /** helper - get the response code given headers **/
  public static  int  getHttpResponseCode(NIOHttpHeaders responseHeaders) { 
    return responseHeaders.getHttpResponseCode();
  }

  /** set various timeout values **/
  public void setDNSTimeout(int timeoutValue) { _dnsTimeout = timeoutValue; } 
  public void setConnectTimeout(int timeoutValue) { _connectTimeout = timeoutValue; }
  public void setDownloadTimeout(int timeoutValue) { _uploadDownloadTimeout = timeoutValue; }
  
  /** get content length / download length **/
  public final int getContentLength() { return _contentLength; }
  public final int getDownloadLength() { return _downloadLength; }
  
  /** check to see if content was truncated **/
  public final boolean isContentTruncated() { return _contentTruncated; }
  
  /** set download max limit - after which content will be trauncated **/
  public final void setDownloadMax(int downloadMax) { _downloadMax = downloadMax; }
  
  /** get the content buffer **/
  public final NIOBufferList getContentBuffer() { return _inBuf; }
  
  /** get the request headers **/
  public final NIOHttpHeaders getRequestHeaders() { return _requestHeaders; }
  /** get the response headers **/
  public final NIOHttpHeaders getResponseHeaders() { return _responseHeaders; }
  
  /** get the time in milliseconds when this connection was opened **/
  public final long  getOpenTime() { return _openTime; }
  
  /** time it took to resolve dns in milliseconds **/
  public final int getResolveTime() { return  _resolveTime; }
  /** time it took to connect **/
  public final int getConnectTime() { return  _connectTime; }
  /** time it took to upload headers and content **/
  public final int getUploadTime()  { return _uploadTime; }
  /** time it took to download content **/
  public final int getDownloadTime() { return _downloadTime; }
  
  /** get / set the connection id **/ 
  public final int getId() { return _id; } 
  public final void setId(int id) { _id = id; }
  
  public final void setUploadRateLimiter(BandwidthUtils.RateLimiter rateLimiter) { 
    _uploadRateLimiter = rateLimiter;
  }
  
  /** get the assigned (active url) **/
  public final URL getURL()          { 
    return _url; 
  }
  
  
  /** set resolved address - don't set this **/
  public void setResolvedAddress(InetAddress address,long ttl,String optionalCName) {
    _resolvedAddress = address;
    _resolvedAddressTTL = ttl;
    _resolvedCName = optionalCName;
  }
  
  /** get resolved address **/
  public InetAddress getResolvedAddress() { return _resolvedAddress; } 
  
  /** resolved address ttl **/
  public long          getResolvedAddressTTL() { return _resolvedAddressTTL; }
  /** resolved cname **/
  public String          getResolvedServerCName() { return _resolvedCName; }
  
  
  public final Exception getLastException() { return _lastException; }
  
  public final NIOClientSocket getSocket() { return _socket; } 

  
  public void open() throws IOException { 
    
    if (_state != State.IDLE)
      throw new IOException("Invalid State");
    
    _openTime = System.currentTimeMillis();
    
    if (_url.getHost().length() == 0 || _method.length() == 0 || _httpVersionString.length() == 0)
      throw new IOException("Invalid Base HTTP Parameters Specified");
    
    setState(State.AWAITING_RESOLUTION,null);
    
    InetSocketAddress socketAddress = getProxyServer();
    
    if (socketAddress == null) { 
      InetAddress addressToConnectTo = null;
      
      // get host name ... 
      String hostName = _url.getHost();
  
      // figure out if it url is an explicit IP Address ... 
      byte[] ipAddress = IPAddressUtils.textToNumericFormatV4(hostName); 
        
      // if this IS an IP address (vs a hostname that needs to be resolved...)
      if (ipAddress != null) { 
        // set address to connect to ... 
        addressToConnectTo = InetAddress.getByAddress(ipAddress);
      }
      // now if address to connect to is still null... 
      if (addressToConnectTo == null) { 
        // see if someone overloaded resolved address ... 
        addressToConnectTo = getResolvedAddress();
      }
      
      // now if address to connect to is not null, convert it to a socket address 
      if (addressToConnectTo != null) { 
        //TODO: FIX THIS FOR HTTPS
        socketAddress = new InetSocketAddress(addressToConnectTo,(_url.getPort() == -1) ? 80 : _url.getPort());
      }
    }
    // now, if socket address is NOT null, directly connect to the specified address, bypassing dns lookup 
    if (socketAddress != null) {
      startConnect(socketAddress);
    }
    // otherwise delegate to resolver (to figure out ip address)
    else {
      LOG.info("Sending Host:" + _url.getHost() + " to resolver");
      _resolver.resolve(this,_url.getHost(),false,true,_dnsTimeout);
    }
  }

  public void close() { 
    
    if (!_closed) {
      
      if (_socket != null) {
        _selector.cancelRegistration(_socket);
        _socket.close();
      }
      // calc stats ... 
      if (getState() == State.DONE) {
        _downloadLength = (int)_inBuf.available();
      }
      else { 
        // if not in a done state when closing ... release content buffer ..
        _inBuf.reset();
      }
      // release output buffer ... 
      _outBuf.reset();
      
      _closed = true;
    }
  }
  
  private final void  _buildAndWriteRequestHeader()throws IOException { 
  
    if (_populateDefaultHeaderItems) { 
      
      String file = null;
      if (getProxyServer() == null) { 
        file = _url.getPath();
        if (_url.getQuery()  !=  null) { 
          file += "?";
          file += _url.getQuery();
        }
      }
      else {  
        file = _url.toString();
        //LOG.info("!!!! Proxy Server Set. Using fully qualified URI:" + _url.toString());
      }
      
      if (file.length() == 0)
        file = "/";
      _requestHeaders.prepend(_method + " " + file +" "  +_httpVersionString, null);
  
      //TODO: FIX FOR HTTPS
      if (_url.getPort() != -1 && _url.getPort() != 80) { 
        _requestHeaders.setIfNotSet("Host",_url.getHost() +":"+String.valueOf(_url.getPort()));
      }
      else { 
        _requestHeaders.setIfNotSet("Host",_url.getHost());
      }
      
      _requestHeaders.setIfNotSet ("User-Agent", _defaultUserAgentString);
      _requestHeaders.setIfNotSet ("Accept", "text/html,application/xhtml+xml,text/xml;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5");
      _requestHeaders.setIfNotSet ("Accept-Language","en-us,en;q=0.5");
      _requestHeaders.setIfNotSet ("Accept-Encoding","gzip");
      _requestHeaders.setIfNotSet ("Accept-Charset","ISO-8859-1,utf-8;q=0.7,*;q=0.7");
      _requestHeaders.setIfNotSet ("Connection", "close");
      _requestHeaders.setIfNotSet ("Cache-Control", "no-cache");
      _requestHeaders.setIfNotSet ("Pragma", "no-cache");
      
      // if the cookie store is available ... 
      if (_cookieStore != null) { 
        // query cookie store 
        String cookies = _cookieStore.GetCookies(_url);
        // if cookies available ... set them ... 
        if (cookies.length() != 0) {
          if (_cookieLogger != null) { 
            _cookieLogger.info("Got Cookies:" + cookies + " from URL:" + _url);
          }
          _requestHeaders.setIfNotSet("Cookie", cookies);
        }
      }
    }
    
    NIOStreamEncoder encoder = new NIOStreamEncoder(_outBuf,_utf8Charset.newEncoder());
    PrintWriter writer = new PrintWriter(encoder);
    _requestHeaders.print(writer);
    writer.flush();
    _outBuf.flush();
  }
  
  //@Override
  public void AddressResolutionFailure(NIODNSResolver eventSource,String hostName,Status status,String errorDesc){ 
    
    // LOG.error("Address Resolution FAILED for:" + hostName + " Status:"+ status + " Desc:" + errorDesc);
    
    // set up error specifics BEFORE changing state to ERROR 
    if (status == Status.RESOLVER_FAILURE)
      setErrorType(ErrorType.RESOLVER_FAILURE);
    else if (status == Status.SERVER_FAILURE)
      setErrorType(ErrorType.DNS_FAILURE);
    setErrorDesc(errorDesc);
    
    setState(State.ERROR, new java.net.UnknownHostException(hostName));
  }
  
  //@Override
  public void AddressResolutionSuccess(NIODNSResolver eventSource,String hostName,String cName, InetAddress address,long addressTTL){

    // LOG.info("AddressResolution for Host:" + hostName + " returned TTL:" + addressTTL);
    
    _resolvedAddress = address;
    _resolvedAddressTTL = Math.max(addressTTL,System.currentTimeMillis() + MIN_DNS_CACHE_TIME);
    _resolvedCName = cName;

    //TODO: FIX FOR HTTPS
    // start the actual connect ... 
    startConnect(new InetSocketAddress(_resolvedAddress,(_url.getPort() == -1) ? 80 : _url.getPort()));
  }

  private void startConnect(InetSocketAddress addressToConnectTo) { 
    
    if (_socket != null && _socket.isOpen()) { 
      
      setState(State.AWAITING_CONNECT,null);
    
      try {
        
        _buildAndWriteRequestHeader();
        _socket.connect(addressToConnectTo);
        _selector.registerForConnect(_socket);
      }
      catch (IOException e) {
        LOG.error("Socket Connect via Address Resolution Success for host:"+ _url.getHost() + " threw Exception:" + e.getMessage());

        setErrorType(ErrorType.IOEXCEPTION);
        setErrorDesc(e.toString());
        setState(State.ERROR,e);
      }
    }
    else { 
      LOG.error("AddressResolutionSuccess called on closed connection. URL:" +_url );

      setErrorType(ErrorType.UNKNOWN);
      setErrorDesc("AddressResolutionSuccess called on closed connection. URL:" +_url );
      setState(State.ERROR, new java.net.ConnectException(_url.getHost()));
    }
  }
  
  public void DNSResultsAvailable() { 
    if (_selector != null) { 
      try { 
        _selector.wakeup();
      }
      catch (IOException e) {

      }
    }
  }
  
  //@Override
  public void Connected(NIOClientSocket theSocket) throws IOException { 
   
    setState(State.SENDING_REQUEST,null);
    
    try { 
      _selector.registerForWrite(_socket);
    }
    catch (IOException e ){
      LOG.error("registerForWrite for url:" + getURL() + " threw Exception:" +e.getMessage());

      setErrorType(ErrorType.IOEXCEPTION);
      setErrorDesc(e.toString());
      setState(State.ERROR,e);
    }
  }
  
  //@Override
  public void Disconnected(NIOSocket theSocket,Exception disconnectReason) throws IOException{
    if (_state == State.RECEIVING_CONTENT) { 
      setState(State.DONE,null);
    }
    else if (_state != State.DONE && _state != State.ERROR) { 
      setErrorType(ErrorType.IOEXCEPTION);
      if (disconnectReason != null) { 
        setErrorDesc("Disconnected with State:" + getState() + " And Exception:" + disconnectReason.toString());
      }
      else { 
        setErrorDesc("Disconnected with State:" + getState() + " ContentLength:" + _contentLength + " BufferSize:" + _inBuf.available());
      }
      setState(State.ERROR,(disconnectReason != null) ? disconnectReason : new java.net.SocketException());
    }
  }
  
  //@Override
  public void Excepted(NIOSocket s,Exception e){
    LOG.error("Caught Unhandled Exception:" + StringUtils.stringifyException(e) + " for URL:" + getURL());
    setErrorType(ErrorType.IOEXCEPTION);
    setErrorDesc("Unhanled Exception:" + StringUtils.stringifyException(e));
    setState(State.ERROR,e);
  }
  
  
  //@Override
  public void Writeable(NIOClientSocket theSocket) throws IOException{ 

    if (!theSocket.isOpen()){
      return;
    }
    
    int  amountWritten = 0;
    
    try { 

      boolean contentEOF = false;
      
         
       amountWritten = 0;
       
       if (_outBuf.available() == 0 && _dataSource != null) { 
         // read some more data from the data source 
         contentEOF = _dataSource.read(_outBuf);
       }
       
       ByteBuffer bufferToWrite = _outBuf.read();
       
       if (bufferToWrite != null) { 
         
         try {
           
           int amountToWrite = bufferToWrite.remaining();
           
           // if upload rate limiter is not null ... 
           if (_uploadRateLimiter != null) { 
             // apply rate limit policy to outbound data ... 
             amountToWrite = _uploadRateLimiter.checkRateLimit(amountToWrite);
           }
           
           if (amountToWrite != 0) { 
             // if amount to write is less than remaining ... 
             if (amountToWrite < bufferToWrite.remaining()) { 
               //slice the buffer ... 
               ByteBuffer slicedBuffer = bufferToWrite.slice();
               // limit to amount to write ... 
               slicedBuffer.limit(amountToWrite);
               // and write to socket ... 
               amountWritten = _socket.write(slicedBuffer);
               if (amountWritten >=0 ) { 
                 // advance source buffer manually...
                 bufferToWrite.position(bufferToWrite.position() + amountWritten);
               }
             }
             else { 
               amountWritten = _socket.write(bufferToWrite);
             }
             
             if (_uploadRateLimiter != null) { 
               _uploadRateLimiter.updateStats(amountWritten);
               
               // debug output ... 
               BandwidthUtils.BandwidthStats stats = new BandwidthUtils.BandwidthStats();
               // collect stats 
               _uploadRateLimiter.getStats(stats);
               // dump stats ... 
               // System.out.println("Connection: "+ this+"Upload Speed:" + stats.scaledBitsPerSecond + " " + stats.scaledBitsUnits + " TotalWritten:" + (_cumilativeWritten + amountWritten) );
               // LOG.info("Connection:" + getId()+" BytesOut:" + amountWritten +" Upload Speed:" + stats.scaledBitsPerSecond + " " + stats.scaledBitsUnits + " TotalWritten:" + (_cumilativeWritten + amountWritten));
             }
           }
         }
         catch (IOException exception) { 
           // LOG.error(CCStringUtils.stringifyException(e));
           throw exception;
         }
         _totalWritten += amountWritten;
         _cumilativeWritten += amountWritten;
  
         // System.out.println("NIOHttpConnection->wrote:" + amountWritten + "Bytes TotalWritten:" + _cumilativeWritten);
  
         
         if (bufferToWrite.remaining() > 0 ) { 
             _outBuf.putBack(bufferToWrite);
         }
       }
      
      if (_totalWritten > 0 && !_outBuf.isDataAvailable() && (_dataSource == null || contentEOF)) { 
        
        _lastReadOrWriteTime = System.currentTimeMillis();
        
        // transition from sending to receiving ... 
        if (_state == State.SENDING_REQUEST) {
          // set up an initial last read time value here ... 
          setState(State.RECEIVING_HEADERS,null);
          _selector.registerForRead(theSocket);
        }
      }
    }
    catch (IOException e) {
      LOG.error("Writeable for url:" + getURL() + " threw Exception:" +e.getMessage());

      setErrorType(ErrorType.IOEXCEPTION);
      setErrorDesc(StringUtils.stringifyException(e));
      setState(State.ERROR,e);
      
      throw e;
    }
    
    if (_state == State.SENDING_REQUEST) { 
      _selector.registerForReadAndWrite(theSocket);
    }
    else if (_state.ordinal() >= State.RECEIVING_HEADERS.ordinal() && _state.ordinal() <State.DONE.ordinal()) { 
      _selector.registerForRead(theSocket);
    }
  }
  
  /** internal routine that processes incoming data **/
  final void processIncomingData(int newBytes)throws IOException{
    
    // now loop over remaining data ...
    while (_inBuf.isDataAvailable()) { 
    
      if (_state == State.RECEIVING_HEADERS) {
        // attempt to process headers
        if (!processHeaders()) { 
          break;
        }
      }
      else if (_state == State.RECEIVING_CONTENT) { 
        
        if (_chunked) { 
          processChunkedContent();
        }
        else {
          
          processUnChunkedContent(Math.min(newBytes,_inBuf.available()));
        }
        break;
      }
      else { 
        break;
      }
    }    
  }
  
//@Override
  public int Readable(NIOClientSocket theSocket) throws IOException{ 
    
    if (!theSocket.isOpen()){
      LOG.error("Readable Called on Closed Socket");
      return -1;
    }
    
    int totalBytesRead = 0;
    int singleReadAmount = 0;
    boolean overflow = false;
    boolean disconnected = false;
    
    try {
      
      if (_downloadMax == -1 || _totalRead < _downloadMax ) { 
        
        do { 
        
          ByteBuffer buffer = _inBuf.getWriteBuf();
        
          if (_downloadMax != -1) { 
            if (_totalRead + buffer.remaining() > _downloadMax) {
              int overflowAmt = (_totalRead + buffer.remaining()) - _downloadMax;
              buffer.limit(buffer.limit() - overflowAmt);
            }
          }
          
          singleReadAmount = _socket.read(buffer);
          
          if (singleReadAmount > 0) { 
            _inBuf.write(buffer);
            _totalRead += singleReadAmount;
            _cumilativeRead += singleReadAmount;
            totalBytesRead += singleReadAmount;
          }
        
        } while (singleReadAmount > 0 && (_downloadMax == -1 || _totalRead < _downloadMax) );
        
        if (_downloadMax != -1 && _totalRead == _downloadMax) { 
          overflow = true;
          _contentTruncated = true;
        }
      }
      
      if (totalBytesRead > 0) { 
        // flush any written buffers .
        _inBuf.flush();
        // process incoming buffer 
        processIncomingData(totalBytesRead);
      }
        
      
      if (singleReadAmount == -1 || overflow) {
        
        disconnected = true;
        
        if (getState() == State.RECEIVING_CONTENT && (overflow || _contentLength == -1 || _contentLength == _downloadedContentLength) ) { 
          
          // if we are still in the middle of processing chunked data ... 
          if (_chunked) { 
            // clear out existing input buffer ...
            _inBuf.reset();
            // and if a chunk buffer is available ... 
            if (_chunkContentBuffer != null) { 
              // take what we can get ... 
              
              // flush chunk buffer ...
              _chunkContentBuffer.flush();
              // and swap it with the real content buffer ... 
              _inBuf = _chunkContentBuffer;
              // reset chunk state ... 
              _chunkContentBuffer = null;
            }
            // reset chunked flag ... 
            _chunked = false;

            // and now, if this is NOT an overflow condidition ... 
            if (!overflow) { 
              // interpret this as an error ...
              setErrorType(ErrorType.IOEXCEPTION);
              setErrorDesc("Connection Closed Before Receiving Chunk Trailer");
              setState(State.ERROR,new java.net.SocketException());
            }
          }
          
          // now check one more time of we are are in the proper state ... 
          if (getState() == State.RECEIVING_CONTENT) { 
            setState(State.DONE,null);
          }
        }
        else if (getState() != State.DONE){
          if (getState() == State.RECEIVING_CONTENT && _downloadedContentLength != 0) { 
            LOG.warn("URL:" + _url + " POSSIBLE TRUNCATION: Read returned -1 with ContentLength:" + _contentLength + " BufferSize:" + _inBuf.available() + " DownloadSize:" + _downloadedContentLength + " State:" + getState() + "Context:" + _context);
            setState(State.DONE,null);
          }
          else { 
            LOG.error("URL:" + _url + " Read returned -1 with ContentLength:" + _contentLength + " BufferSize:" + _inBuf.available() + " DownloadSize:" + _downloadedContentLength + " State:" + getState() + "Context:" + _context);
            
            setErrorType(ErrorType.IOEXCEPTION);
            setErrorDesc("Read returned -1 with ContentLength:" + _contentLength + " BufferSize:" + _inBuf.available() + " DownloadSize:" + _downloadedContentLength + " State:" + getState());
            setState(State.ERROR,new java.net.SocketException());
          }
        }
      }
    }
    catch (IOException e) { 
      LOG.error("Readable for url:" + getURL() + " threw Exception:" +e.getMessage());

      setErrorType(ErrorType.IOEXCEPTION);
      setErrorDesc(StringUtils.stringifyException(e));

      setState(State.ERROR,e);
      
    }
    if (_socket.isOpen()) { 
      // if we data to write ... 
      if (_outBuf.isDataAvailable()) { 
        _selector.registerForReadAndWrite(theSocket);
      }
      else {
        _selector.registerForRead(theSocket);
      }
    }
    
    if (totalBytesRead > 0) { 
      //update last read time ...
      _lastReadOrWriteTime = System.currentTimeMillis();
    }
    
    return (disconnected)  ? -1 : totalBytesRead;
  }
  
  private boolean accumulateHeaders(int headersMax) throws IOException {
    
    if (_incomingAccumulationBuffer == null) { 
      _incomingAccumulationBuffer = new ByteArrayOutputStream(HTTP_HEADER_SIZE_MAX);
    }
    
    
    ByteBuffer currentBuffer = null;
    boolean    eolFound      = false;
    
    
    while (!eolFound && (currentBuffer = _inBuf.read()) != null) { 
      
      
      while (!eolFound && currentBuffer.hasRemaining()) { 
        
        byte c = currentBuffer.get();
        
        _incomingAccumulationBuffer.write(c);
        
        if (c == '\n') {
          if (_lastCharWasLF) { 
            eolFound = true;
          }
          else {
            _lastCharWasLF = true;
          }
        } else if (c != '\r' || _lastChar != '\n') {
          _lastCharWasLF = false;
        }
        _lastChar = c;
        
        if (eolFound) {
          if (currentBuffer.hasRemaining()) {
            // if trailing data in buffer , push it back for content phase
            _inBuf.putBack(currentBuffer);
          }
          return true;
        }
        else {
          if (headersMax != -1 && _incomingAccumulationBuffer.size() > headersMax)
            throw new IOException("Header Size Limit Reached With No Terminator!");
        }
      }
    }
    return false;
  }

  private static final int MIN_BYTES_FOR_STATUS_LINE = 8;
  private static final int HTTP_STATUS_LINE_SLOP = 4;
  private static byte[] httpStr = { 'h','t','t','p' };
  
  
  private static boolean isHTTPToken(byte[] data,int offset,int length) { 
    if (length >= httpStr.length) { 
      for (int i=0;i<httpStr.length;++i) { 
        if (data[offset + i] != httpStr[i] && data[offset + i] + 32 != httpStr[i]) { 
          return false;
        }
      }
      return true;
    }
    return false;
  }
  
  private boolean detectStatusLine() throws IOException {
    
    boolean detectedStatusLine = false;
    if (_inBuf.available() >= httpStr.length) { 
      
      byte statusLineBytes[] = new byte[8];
      
      NIOBufferListInputStream temp = new NIOBufferListInputStream(_inBuf);
      temp.read(statusLineBytes);
      // ok close the stream so remaining buffer goes back to list :-(
      temp.close();
      // ok now sniff status line bytes
      
      for (int i=0;i<=3;++i) { 
        if (isHTTPToken(statusLineBytes, i, 4)) { 
          detectedStatusLine = true;
          break;
        }
      }
      
      boolean addBytesAsNewByteBuffer = true;
      // ok unwind the read operation ... 
      ByteBuffer nextReadBuffer = _inBuf.read();
      // most common case ... 
      if (nextReadBuffer != null) {
        if (nextReadBuffer.position() == 8) { 
          // unwind the read by cursor repositioning 
          nextReadBuffer.position(0);
          addBytesAsNewByteBuffer = false;
        }
        // put the buffer back 
        _inBuf.putBack(nextReadBuffer);
      }
      if (addBytesAsNewByteBuffer) { 
        _inBuf.putBack(ByteBuffer.wrap(statusLineBytes));
      }
    }
    return detectedStatusLine;
  }

  
  private boolean processHeaders() throws IOException {
    
    if (!_foundStatusLine) { 
      // ok check to see if we have minimum amount of data necessary to parse status line ... 
      if (_inBuf.available() < MIN_BYTES_FOR_STATUS_LINE) { 
        return false;
      }
      else { 
        _foundStatusLine = detectStatusLine();
        if (!_foundStatusLine) { 
          // ok assume this is http 0.9
          LOG.info("No stats line found while process headers. Assuming http 0.9 response");
          _responseHeaders.add(null, "HTTP-0.9 200 OK");
          setState(State.RECEIVING_CONTENT,null);
        }
      }
    }
    
    if (_foundStatusLine) { 
      if (accumulateHeaders(HTTP_HEADER_SIZE_MAX)) {
        
        setState(State.PARSING_HEADERS,null);
  
        // now parse headers ... 
        Reader reader  = new InputStreamReader(new ByteArrayInputStream(_incomingAccumulationBuffer.toByteArray()),Charset.forName("UTF-8"));
        
        _responseHeaders.mergeHeader(reader);
        
        // check to see if cookie store is available ... 
        if (_cookieStore != null) { 
          Iterator<String> values = _responseHeaders.multiValueIterator("Set-Cookie");
          while(values.hasNext()) { 
            String value = values.next();
            if (value != null && value.length() != 0){
              if (_cookieLogger != null) { 
                _cookieLogger.info("Setting Cookie:" + value + " to url:" + _url);
              }
              _cookieStore.setCookie(_url, value);
            }
          }
        }
        
        // check to see if content length was specified ... 
        String strContentLength =_responseHeaders.findValue("Content-Length");
        if (strContentLength != null) { 
          try { 
            _contentLength = Integer.parseInt(strContentLength);
          }
          catch (NumberFormatException e) { 
            LOG.error("Number Format Exception parsing Content-Length:" + strContentLength);
          }
        }
        
        
        setState(State.RECEIVING_CONTENT,null);
        
        // check to see if content is using chunked transfer encoding ... 
        String strTransferEncoding = _responseHeaders.findValue("Transfer-Encoding");
        
        if(strTransferEncoding != null) { 
          
          if (strTransferEncoding.equalsIgnoreCase("CHUNKED") ) {
            // ignore content length if specified ...
            _contentLength = -1;
            _chunked = true;
            _chunkState = ChunkState.STATE_AWAITING_CHUNK_HEADER;
            _chunkLineBuffer = new StringBuffer(CHUNK_LINE_MAX);
            _chunkContentBuffer = new NIOBufferList();
          }
          else { 
            LOG.error("Unknown Transfer Encoding in Response Headers:" + strTransferEncoding);
            throw new IOException("Uknown Transfer Encoding");
          }
        }
        
        if (_contentLength == 0) { 
          setState(State.DONE,null);
        }
        return true;
      }
    }
    return false;
  }
  

  private void processChunkedContent() throws IOException { 

    while (_inBuf.available() != 0 && _chunkState != ChunkState.STATE_DONE) { 

      switch (_chunkState) { 
      
        case STATE_AWAITING_CHUNK_HEADER: { 
          
          _chunkCRLFReadState = _inBuf.readCRLFLine(_chunkLineBuffer, CHUNK_LINE_MAX, _chunkCRLFReadState);
          
          if (_chunkCRLFReadState == CRLFReadState.DONE) {
            // get the newly extracted line ... 
            String line = _chunkLineBuffer.toString();
            // now find first occurence of whitespace ... 
            int whiteSpaceIdx = line.indexOf(' ');
            if (whiteSpaceIdx != -1) { 
              line = line.substring(0, whiteSpaceIdx);
            }
            // now extract chunk length ... 
            try {
              _chunkSize = Integer.parseInt(line, 16);
            } 
            catch (NumberFormatException e) {
              LOG.error("Invalid Chunk Size Encountered reading CHUNK HEADER:" + line);
              throw new IOException("Invalid chunk size");
            }
            // reset chunk pos cursor ... 
            _chunkPos    = 0;
            // reset chunk read state 
            _chunkCRLFReadState = CRLFReadState.NONE;
            // reset the buffer for the next potential line read ... 
            _chunkLineBuffer.setLength(0);
            
            // now interpret the chunk size value ... 
            if (_chunkSize > 0) { 
              _chunkState = ChunkState.STATE_READING_CHUNK;
            }
            else { 
              _chunkState  = ChunkState.STATE_AWAITING_TRAILERS;
            }
          }
        }
        break;
        
        case STATE_READING_CHUNK: { 
          
          // calculate amount we want to read in ... 
          int amountToRead = Math.min(_chunkSize - _chunkPos, _inBuf.available());
          // and track amount we wrote into chunk content buffer 
          int amountWritten = 0;
          
          while (amountToRead != 0) { 

            // get a write buffer ... 
            ByteBuffer writeBuffer = _chunkContentBuffer.getWriteBuf();
            
            // get the next read buffer 
            ByteBuffer readBuffer = _inBuf.read();
            
            if (readBuffer == writeBuffer) { 
              throw new RuntimeException("BAD NEWS!!!");
            }
            
            //TODO: There is an opportunity here to skip buffer copy altogether and add read buffer directly to write buffer list 
            //            Need to look into this. 
            
            // if buffer size is > amountToRead ... 
            if (readBuffer.remaining() > writeBuffer.remaining() || readBuffer.remaining() > amountToRead) { 
              
              // slice the read buffer ... 
              ByteBuffer sliced = readBuffer.slice();
              // calculate slice amount 
              int sliceAmount = Math.min(writeBuffer.remaining(), amountToRead);
              
              // and increment original ... 
              readBuffer.position(readBuffer.position() + sliceAmount);
              // and limit sliced buffer scope ... 
              sliced.limit(sliced.position() + sliceAmount);
              // reduce amountToRead 
              amountToRead -= sliceAmount;
              // and increment chunk pos 
              _chunkPos += sliceAmount;
              // track amount written ... 
              amountWritten += sliced.remaining();
              // append it ... 
              writeBuffer.put(sliced);
              // and put back the read buffer 
              _inBuf.putBack(readBuffer);
            }
            // otherwise... append whole buffer to write buffer 
            else { 
              // reduce amountToRead 
              amountToRead -= readBuffer.remaining();
              // and increment chunk pos 
              _chunkPos += readBuffer.remaining();
              // track amount written 
              amountWritten += readBuffer.remaining();
              // append as much as possible into the write buffer ... 
              writeBuffer.put(readBuffer);
            }
          }
          
          // if we wrote some data to the content buffer ... 
          if (amountWritten != 0) { 
            // update bytes downloaded ...
            _downloadedContentLength += amountWritten;
            
            if (getListener() != null) { 
              // inform listener of content availability 
              getListener().HttpContentAvailable(this, _chunkContentBuffer);
            }
          }
          
          // now if we read in a chunks worth of data ... advance state ... 
          if (_chunkPos == _chunkSize) { 
            _chunkState = ChunkState.STATE_AWAITING_CHUNK_EOL;
          }
        }
        break;
        
        
        case STATE_AWAITING_CHUNK_EOL: { 
         
          if (_inBuf.available() >= 2) { 
            ByteBuffer readBuffer = _inBuf.read();
            
            if (readBuffer.get() != '\r')  { 
                LOG.error("Missing CR from Chunk Data Terminator");
                throw new IOException("missing CR");
            }
            // now if read buffer is expended ... release it and get another one ... 
            if (readBuffer.remaining() == 0) {
              readBuffer = _inBuf.read();
            }
            
            if (readBuffer.get() != '\n') {
                LOG.error("Missing LFfrom Chunk Data Terminator");
                throw new IOException("missing LF");
            }
            // put back the read buffer 
            _inBuf.putBack(readBuffer);
            // and transition to the next state ... 
            _chunkState = ChunkState.STATE_AWAITING_CHUNK_HEADER;
          }
          else { 
            // break out and wait for more data 
            return;
          }
        }
        break;
        
        case STATE_AWAITING_TRAILERS: { 
          
          _chunkCRLFReadState = _inBuf.readCRLFLine(_chunkLineBuffer, CHUNK_LINE_MAX, _chunkCRLFReadState);
          
          if (_chunkCRLFReadState == CRLFReadState.DONE) {
            // transition to a done state ... 
            _chunkState = ChunkState.STATE_DONE;
            // clear out intermediate crlf state
            _chunkCRLFReadState = CRLFReadState.NONE;
            _chunkLineBuffer.setLength(0);
          }
          else { 
            break;
          }
        }
        // fall through if chunk state is done ... 
        
        case STATE_DONE: { 
          // clear out existing input buffer ...
          _inBuf.reset();
          // flush chunk buffer ...
          _chunkContentBuffer.flush();
          // and swap it with the real content buffer ... 
          _inBuf = _chunkContentBuffer;
          // reset chunk state ... 
          _chunkContentBuffer = null;
          // reset chunked flag ... 
          _chunked = false;
          // set HTTP DONE state ... 
          setState(State.DONE,null);
        }
        break;
      }
    }
  }
  
  private boolean processUnChunkedContent(int newBytesIn) throws IOException  { 
    
    // recalculate downloaded content length  
    _downloadedContentLength += newBytesIn;

    
    // call listener if required  
    if (_downloadedContentLength != 0 && getListener() != null) { 
      getListener().HttpContentAvailable(this,_inBuf);
    }
    // now if content length is specified and download length == content length, we are done ..
    if (_contentLength != -1 && _downloadedContentLength >= _contentLength){
      setState(State.DONE,null);
    }
    return true;
  }
    

  public boolean hasTimedOut() { 
  
    boolean timedOut = false;
    if (getState().ordinal() >   State.AWAITING_RESOLUTION.ordinal()) { 
      
      timedOut = (getState() == State.ERROR && getErrorType() == ErrorType.TIMEOUT);
      
      if (!timedOut && getState().ordinal() < State.DONE.ordinal()) { 
      
        long currentTime = System.currentTimeMillis();
        long timeDelta = 0;
        
        if (getState().ordinal() <= State.AWAITING_CONNECT.ordinal()) { 
          timeDelta = currentTime - _phaseStartTime;
          timedOut =  (timeDelta >= _connectTimeout);
        }
        else {
          timeDelta = currentTime - _lastReadOrWriteTime;
          timedOut = (timeDelta >= _uploadDownloadTimeout);
        }
        
        if (timedOut) { 
  
          _timeoutState = getState();
          State oldState = getState();
          
          // we want to disable callbacks here, since we are returning error state to caller ... 
          Listener listenerTemp = _listener;
          _listener = null;
  
          setErrorType(ErrorType.TIMEOUT);
          setErrorDesc("TIMEOUT-IN STATE:"+oldState.toString());
  
          setState(State.ERROR,new java.net.SocketTimeoutException());
          // restore listener here ... 
          _listener = listenerTemp;
          timedOut = true;
        }
      }
    }
    return timedOut;
  }

  @Override
  public String toString() {
    String strOut = "State:" + _state + "\n";
    if (_url != null) { 
      strOut += "URL:" + _url.toString() + "\n";
    }
    if (_context != null) { 
      strOut += "ContextObj:" + _context + "\n";
    }
    return strOut;
  };
  
  
  @Override
  public void done(NIODNSResolver eventSource,FutureTask<NIODNSQueryResult> task) {
    // TODO Auto-generated method stub
    
  }
  
  /********************************************************************/
  // UNIT TEST SUPPORT
  /********************************************************************/
  
  void mockRead(String line) throws IOException { 
    byte[] bytes = line.getBytes();
    ByteBuffer incomingBuffer = ByteBuffer.wrap(bytes);
    incomingBuffer.position(bytes.length);
    _inBuf.write(incomingBuffer);
    _inBuf.flush();
    processIncomingData(bytes.length);
  }
  
  void mockConnectionClose()throws IOException { 
    // now check one more time of we are are in the proper state ... 
    if (getState() == State.RECEIVING_CONTENT) { 
      setState(State.DONE,null);
    }    
  }
  
  
  public static class NIOHttpConnectionUnitTest { 

    @Test
    public void runTest() throws Exception {
      testHTTPConnection();
    }
  };
  
  static void testHTTPConnection() { 
    // basic http 
    Assert.assertTrue(MockHTTPConnection(
        new String[] { "HTTP/1.0 200 OK\r\n\r\n","hello world"},false,"HTTP/1.0 200 OK",200,"hello world"));
    // basic http no header ... 
    /*
    Assert.assertTrue(MockHTTPConnection(
        new String[] { "hello world" },false,"HTTP/0.9 200 OK",200,"hello world"));
    */ 
    // 404 with alternate line terminators ... 
    
    Assert.assertTrue(MockHTTPConnection(
        new String[] { "HTTP/1.0 404 Not Found\nServer: blah\n\nDATA" },false,"HTTP/1.0 404 Not Found",404,"DATA"));
    
    // streaming headers ... 
    
    Assert.assertTrue(MockHTTPConnection(
        new String[] { "HTTP/1.0 ","200 OK","\n","Server: blah\r","\n","\n","DA","TA" },false,"HTTP/1.0 200 OK",200,"DATA"));

  }
  
  static boolean MockHTTPConnection(String[] dataSet,boolean failureExcepted,String statusLineExpected,int statusCodeExpected,String dataExpected) { 
      
    NIOHttpConnection connection = new NIOHttpConnection();
    
    connection._state = State.RECEIVING_HEADERS;
    
    try {
      for (String dataLine : dataSet) {
        connection.mockRead(dataLine);
      }
      connection.mockConnectionClose();
    }
    catch (IOException e) { 
      LOG.error(StringUtils.stringifyException(e));
      if (failureExcepted) { 
        return true;
      }
      else{ 
        return false;
      }
    }
    
    if (failureExcepted) { 
      return false;
    }
    else { 
      NIOHttpHeaders headers = connection.getResponseHeaders();
      
      if (headers.getValue(0).compareTo(statusLineExpected) != 0) { 
        LOG.error("Status Line Comparison Failed. Connection:" + headers.getValue(0) + " Expected:" + statusLineExpected);
        return false;
      }
      if (getHttpResponseCode(headers) != statusCodeExpected) { 
        LOG.error("Status Code Different. Found:" + getHttpResponseCode(headers) + " Expected:" + statusCodeExpected);
        return false;  
      }
      
      byte[] bytesExpected = dataExpected.getBytes();
      
      int bytesAvailable = connection.getContentBuffer().available();
      
      if (bytesExpected.length == bytesAvailable) { 
        byte[] bytesFetched = new byte[bytesExpected.length];
        try {
          connection.getContentBuffer().read(bytesFetched);
          if (!Arrays.equals(bytesExpected,bytesFetched)) { 
            LOG.error("Content Mismatch!");
            return false;
          }
        } catch (IOException e) {
          LOG.error(StringUtils.stringifyException(e));
          return false;
        }
      }
      else { 
        LOG.error("Content Length Mismatch!");
        return false;
      }
    }
    return true;
  }
  
  public void disableReads() { 
    if (_socket != null) { 
      _socket.disableReads();
    }
  }
  
  public void enableReads()throws IOException { 
    if (_socket != null && _socket.isOpen()) { 
      _socket.enableReads();
      _selector.registerForRead(_socket);
    }
  }

  
}
