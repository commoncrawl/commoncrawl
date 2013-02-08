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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.io.internal.NIODNSCache.Node;
import org.commoncrawl.io.internal.NIODNSQueryClient.Status;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.IPAddressUtils;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.PTRRecord;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.ResolverConfig;
import org.xbill.DNS.ReverseMap;
import org.xbill.DNS.Section;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;
import org.xbill.DNS.WireParseException;

/**
 * NIODNSResolver - Asynchronous DNS Resolver that utilizes dnsjava to send and
 * recieve DNS queries via a pool of worker threads
 * 
 * TODO: POTENTIALLY USE ASYNC IO TO MAKE RESOLVER BLEND BETTER WITH THE REST OF
 * THE ASYNC DESIGN
 * 
 * @author rana
 * 
 */

public final class NIODNSLocalResolver implements NIODNSResolver {

  public static final int MIN_TTL_VALUE = 60 * 20 * 1000;

  /** dns query logger */
  public interface Logger {
    public void logDNSQuery(String hostName, InetAddress address, long ttl,
        String opCName);

    public void logDNSFailure(String hostName, String errorCode);

    public void logDNSException(String hostName, String exceptionDesc);
  }

  /** cache **/
  private static NIODNSCache _dnsCache                       = new NIODNSCache();
  /** inverse cache **/
  private static NIODNSCache _badHostCache                   = new NIODNSCache();

  /** cache hit counter **/
  private long               _cacheHits                      = 0;

  private long               _cacheMisses                    = 0;

  private long               _queryCount                     = 0;

  private static Logger      _logger                         = null;

  public static final int    SERVER_FAIL_BAD_HOST_LIFETIME   = 3600 * 1000;                         // ONE
                                                                                                     // HOUR
                                                                                                     // BY
                                                                                                     // DEFAULT
  public static final int    NXDOMAIN_FAIL_BAD_HOST_LIFETIME = 5 * 60 * 1000;                       // 5
                                                                                                     // minutes

  /** logging **/
  static final Log           LOG                             = LogFactory
                                                                 .getLog(NIODNSLocalResolver.class);

  /** custom completion service **/
  public class DNSExecutorCompletionService implements
      CompletionService<NIODNSQueryResult> {

    private final Executor                              executor;
    private final BlockingQueue<Future<NIODNSQueryResult>> completionQueue;
    private final EventLoop                             eventLoop;

    /**
     * FutureTask extension to enqueue upon completion
     */
    private class QueueingFuture extends FutureTask<NIODNSQueryResult> {
      QueueingFuture(Callable<NIODNSQueryResult> c) {
        super(c);
      }

      QueueingFuture(Runnable t, NIODNSQueryResult r) {
        super(t, r);
      }

      protected void done() {

        // completionQueue.add(this);
        // wake the event loop up ...
        // eventLoop.wakeup();
        eventLoop.setTimer(new Timer(0, false, new Timer.Callback() {

          @Override
          public void timerFired(Timer timer) {

            if (!isCancelled()) {
              NIODNSQueryResult qResult = null;
              try {
                qResult = QueueingFuture.this.get();
              } catch (Exception e) {
                if (e instanceof IOException) {
                  LOG.error(StringUtils.stringifyException(e));
                } else if (e instanceof RuntimeException) {
                  LOG.fatal(StringUtils.stringifyException(e));
                }
              }
              if (qResult != null) {

                NIODNSQueryClient client = qResult.getConnection();

                if (client != null) {

                  if (qResult.success()) {
                    client.AddressResolutionSuccess(qResult._source, qResult
                        .getHostName(), qResult.getCName(), qResult
                        .getAddress(), qResult.getTTL());
                  } else {
                    client.AddressResolutionFailure(qResult._source, qResult
                        .getHostName(), qResult.getStatus(), qResult
                        .getErrorDescription());
                  }
                  client.done(qResult._source, QueueingFuture.this);
                } else {
                  if (qResult.getHostName() != null)
                    LOG
                        .error("Client no Longer Exists for DNS Resolution Request:"
                            + qResult.getHostName());
                  else
                    LOG
                        .error("Client no Longer Exists for DNS Resolution Request");
                }
              }
            } else {
              LOG.error("Future Cancelled!!!!!");
            }
          }

        }));
      }
    }

    /**
     * Creates an ExecutorCompletionService using the supplied executor for base
     * task execution and a {@link LinkedBlockingQueue} as a completion queue.
     * 
     * @param executor
     *          the executor to use
     * @throws NullPointerException
     *           if executor is <tt>null</tt>
     */
    public DNSExecutorCompletionService(EventLoop eventLoop, Executor executor) {
      if (executor == null)
        throw new NullPointerException();
      this.executor = executor;
      this.completionQueue = new LinkedBlockingQueue<Future<NIODNSQueryResult>>();
      this.eventLoop = eventLoop;
    }

    public Future<NIODNSQueryResult> submit(Callable<NIODNSQueryResult> task) {
      if (task == null)
        throw new NullPointerException();
      QueueingFuture f = new QueueingFuture(task);
      executor.execute(f);
      return f;
    }

    public Future<NIODNSQueryResult> submit(Runnable task, NIODNSQueryResult result) {
      if (task == null)
        throw new NullPointerException();
      QueueingFuture f = new QueueingFuture(task, result);
      executor.execute(f);
      return f;
    }

    public Future<NIODNSQueryResult> take() throws InterruptedException {
      return completionQueue.take();
    }

    public Future<NIODNSQueryResult> poll() {
      return completionQueue.poll();
    }

    public Future<NIODNSQueryResult> poll(long timeout, TimeUnit unit)
        throws InterruptedException {
      return completionQueue.poll(timeout, unit);
    }

  }

  /** EventLoop under which this resolver is operating **/
  private EventLoop                    _eventLoop;

  /** thread pool */
  private ExecutorService              _threadPool                    = null;
  /** high priority requests thread pool **/
  private ExecutorService              _highPriorityThreadPool        = null;

  /** completion service - for supporting psuedo async dns query model */
  private DNSExecutorCompletionService _completionService;
  /** high priority completion service */
  private DNSExecutorCompletionService _highPriorityCompletionService = null;

  /** use tcp (vs. udp) for dns queries **/
  private boolean                      _useTCP                        = true;

  /** dns server address **/
  private String                       _dnsServerAddress              = "127.0.0.1";

  /** context object **/
  private Object                       _context;

  /** NIODNSResolver Constructor **/
  public NIODNSLocalResolver(EventLoop eventLoop,
      ExecutorService dnsThreadPool, boolean useTCP) {
    lookupNameServer();
    _eventLoop = eventLoop;
    _threadPool = dnsThreadPool;
    _completionService = new DNSExecutorCompletionService(_eventLoop,
        _threadPool);
    _useTCP = useTCP;
    _dnsCache.enableIPAddressTracking();
  }

  /**
   * NIODNSResolver Constructor - This version takes a server address as a
   * parameter
   **/
  public NIODNSLocalResolver(String serverAddress, EventLoop eventLoop,
      ExecutorService dnsThreadPool, ExecutorService highPriorityThreadPool,
      boolean useTCP) {
    _dnsServerAddress = serverAddress;
    _eventLoop = eventLoop;
    _threadPool = dnsThreadPool;
    _highPriorityThreadPool = highPriorityThreadPool;
    _completionService = new DNSExecutorCompletionService(_eventLoop,
        _threadPool);
    _highPriorityCompletionService = new DNSExecutorCompletionService(
        _eventLoop, _highPriorityThreadPool);
    _useTCP = useTCP;
    _dnsCache.enableIPAddressTracking();
  }

  public int getQueuedItemCount() {
    if (_threadPool != null) {
      return ((ThreadPoolExecutor) _threadPool).getQueue().size();
    }
    return 0;
  }

  public String getName() {
    return _dnsServerAddress;
  }

  public Object getContextObject() {
    return _context;
  }

  public void setContextObject(Object obj) {
    _context = obj;
  }

  /** lookup default nameserver **/
  private final void lookupNameServer() {
    String server = ResolverConfig.getCurrentConfig().server();
    if (server != null) {
      LOG.info("Using NameServer:" + server);
      _dnsServerAddress = server;
    } else {
      LOG.info("No NameServer Found.Using:" + _dnsServerAddress);
    }
  }

  /** set dns server address **/
  public void setDNSServerAddress(String serverAddressInDottedDecimal) {
    _dnsServerAddress = serverAddressInDottedDecimal;
  }

  public static void setLogger(Logger logger) {
    _logger = logger;
  }

  public static Logger getLogger() {
    return _logger;
  }

  public static NIODNSCache getDNSCache() {
    return _dnsCache;
  }

  public static NIODNSCache getBadHostCache() {
    return _badHostCache;
  }

  public long getQueryCount() {
    return _queryCount;
  }

  public long getCacheHitCount() {
    return _cacheHits;
  }

  public long getCacheMissCount() {
    return _cacheMisses;
  }

  private static long TTL_DELTA_MIN = 1000 * 60 * 10; // min ttl lifetime is 10
                                                      // minutes for us

  public static NIODNSQueryResult checkCache(NIODNSQueryClient client,
      String hostName) throws UnknownHostException {

    NIODNSCache.DNSResult result = _dnsCache.getIPAddressForHost(hostName);

    if (result != null) {

      // get delta between time to live and now ...
      long ttlDelta = result.getTTL() - System.currentTimeMillis();

      // if theoretically this ip address has expired
      if (ttlDelta < 0) {
        if (Math.abs(ttlDelta) > TTL_DELTA_MIN) {
          return null;
        }
      }

      /*
       * if (Environment.detailLogEnabled()) {
       * LOG.info("Cache HIT for host:"+hostName
       * +" ip:"+IPAddressUtils.IntegerToIPAddressString
       * (result.getIPAddress())); }
       */

      NIODNSQueryResult queryResult = new NIODNSQueryResult(null, client, hostName);

      queryResult.setAddress(IPAddressUtils.IntegerToInetAddress(result
          .getIPAddress()));
      queryResult.setCName(result.getCannonicalName());
      queryResult.setTTL(result.getTTL());
      queryResult.setStatus(Status.SUCCESS);

      return queryResult;
    }

    // check bad host cache ...
    Node resolvedNode = _badHostCache.findNode(hostName);

    if (resolvedNode != null) {
      // LOG.info("Found in Bad Host Cache:" + hostName + " ttl:" + new
      // Date(resolvedNode.getTimeToLive()));
    }
    // IFF found and the bad node has not expired ...
    if (resolvedNode != null
        && resolvedNode.getTimeToLive() >= System.currentTimeMillis()) {

      // LOG.info("Host:" + hostName + " Identified as Bad Host via Cache");

      NIODNSQueryResult queryResult = new NIODNSQueryResult(null, client, hostName);

      queryResult.setStatus(Status.SERVER_FAILURE);
      queryResult.setErrorDesc("Failed via Bad Host Cache");

      return queryResult;
    }

    return null;
  }

  /** ReverseDNSQueryResult - the end result of a DNS Query operation */
  public static class ReverseDNSQueryResult {

    /** SUCCESS or FAILURE */
    private Status         _status    = Status.RESOLVER_FAILURE;
    /** error description **/
    private String         _errorDesc = "";
    /** the target host name */
    private InetAddress    _targetAddress;
    /** the returned PTR Records */
    private Vector<String> _hostNames = new Vector<String>();

    /** optional expire time (time to wait for DNS Server Response) */
    // private long _timeout = -1;

    /**
     * constructor
     * 
     * @param client
     *          - callback interface into the client
     * @param hostName
     *          - host name associated with this query
     */
    ReverseDNSQueryResult(InetAddress address) {

      _targetAddress = address;
    }

    public final void setStatus(Status theStatus) {
      _status = theStatus;
    }

    public final Status getStatus() {
      return _status;
    }

    public final boolean success() {
      return _status == Status.SUCCESS;
    }

    public String getErrorDescription() {
      return _errorDesc;
    }

    void setErrorDesc(String errorDesc) {
      _errorDesc = errorDesc;
    }

    public final InetAddress getTargetAddress() {
      return _targetAddress;
    }

    public final Vector<String> getHostNames() {
      return _hostNames;
    }

  };

  public ReverseDNSQueryResult doReverseDNSQuery(InetAddress address,
      boolean useTCP, int timeoutValue) {

    // Ask dnsjava for the inetaddress. Should be in its cache.
    Message response = null;
    Exception resolverException = null;

    // check cache first ...
    ReverseDNSQueryResult result = new ReverseDNSQueryResult(address);

    if (true) {

      try {

        // allocate a simple resolver object ...
        SimpleResolver resolver = new SimpleResolver(_dnsServerAddress);

        // use tcp if requested ...
        if (useTCP)
          resolver.setTCP(true);

        // set the timeout ...
        resolver.setTimeout(timeoutValue);

        // create appropriate data structures ...

        Name name = ReverseMap.fromAddress(address);
        Record rec = Record.newRecord(name, Type.PTR, DClass.IN);
        Message query = Message.newQuery(rec);

        // send it off ...
        try {
          response = resolver.send(query);
        } catch (Exception e) {
          LOG.error("Reverse DNS Resolution for:" + address
              + " threw IOException:" + StringUtils.stringifyException(e));
          resolverException = e;
        }

        if (response != null && response.getRcode() == Rcode.NOERROR) {

          // get answer
          Record records[] = response.getSectionArray(Section.ANSWER);

          if (records != null) {

            // walk records ...
            for (Record record : records) {

              // store CName for later use ...
              if (record.getType() == Type.PTR) {
                result.getHostNames().add(
                    ((PTRRecord) record).getTarget().toString());
              }
            }
          }
        }
      } catch (UnknownHostException e) {
        resolverException = e;
      }

      if (response == null) {
        result.setStatus(Status.RESOLVER_FAILURE);
        LOG
            .error("Critical Reverse DNS Failure for host:"
                + address.toString());
        if (resolverException != null) {
          LOG.error(CCStringUtils.stringifyException(resolverException));
          result.setErrorDesc(resolverException.toString());
        }
      } else if (response.getRcode() != Rcode.NOERROR) {
        result.setStatus(Status.SERVER_FAILURE);
        result.setErrorDesc(Rcode.string(response.getRcode()));
      } else if (response.getRcode() == Rcode.NOERROR) {

        if (result.getHostNames().size() != 0) {
          result.setStatus(Status.SUCCESS);
        } else {
          result.setStatus(Status.SERVER_FAILURE);
          result.setErrorDesc("NO PTR RECORDS FOUND");
        }
      }
    }
    // return result ... will be added to completion queue (to be retrieved via
    // poll method)...
    return result;
  }

  private static final int MAX_DNS_RETRIES = 1;

  public NIODNSQueryResult doDNSQuery(NIODNSQueryClient client, String hostName,
      boolean useTCP, boolean noCache, int timeoutValue) {

    // check cache first ...
    NIODNSQueryResult result = null;
    int retryCount = 0;
    boolean retry = false;
    boolean resultViaCache = false;

    do {

      // Ask dnsjava for the inetaddress. Should be in its cache.
      Message response = null;
      InetAddress address = null;
      String cname = null;
      long expireTime = -1;
      Exception resolverException = null;

      _queryCount++;

      try {
        // LOG.info("Checking Cache for Host:" + hostName);
        if (!noCache)
          result = checkCache(client, hostName);

        if (result != null) {
          // increment stats ...
          _cacheHits++;
          resultViaCache = true;
        }
      } catch (UnknownHostException e) {
        if (_logger != null)
          _logger.logDNSException(hostName, StringUtils.stringifyException(e));
      }

      // if not found in cache ... then do full lookup ...
      if (result == null) {

        _cacheMisses++;

        try {

          SimpleResolver resolver = new SimpleResolver(_dnsServerAddress);
          // allocate a simple resolver object ...
          //NIODNSSimpleResolverImpl resolver = new NIODNSSimpleResolverImpl(this, _dnsServerAddress);

          // use tcp if requested ...
          if (useTCP)
            resolver.setTCP(true);

          // set the timeout ...
          resolver.setTimeout(timeoutValue);

          // create appropriate data structures ...
          Name name = Name.fromString(hostName, Name.root);
          Record rec = Record.newRecord(name, Type.A, DClass.IN);
          Message query = Message.newQuery(rec);

          // send it off ...
          try {
            response = resolver.send(query);
          } catch (IOException e) {
            if (_logger != null)
              _logger.logDNSException(hostName, StringUtils
                  .stringifyException(e));

            resolverException = e;
            if (retryCount++ != MAX_DNS_RETRIES) {
              LOG.info("Waiting to Retry Failed DNS Query for:" + hostName);
              try {
                Thread.sleep(200);
              } catch (InterruptedException e1) {
              }
              LOG.info("Retrying Failed DNS Query for:" + hostName);
              retry = true;
            }
          }

          if (response != null && response.getRcode() == Rcode.NOERROR) {

            // get answer
            Record records[] = response.getSectionArray(Section.ANSWER);

            if (records != null) {

              // walk records ...
              for (Record record : records) {

                // store CName for later use ...
                if (record.getType() == Type.CNAME) {
                  cname = ((CNAMERecord) record).getAlias().toString();
                  if (cname != null && cname.endsWith(".")) {
                    cname = cname.substring(0, cname.length() - 1);
                  }
                }
                // otherwise look for A record
                else if (record.getType() == Type.A && address == null) {
                  address = ((ARecord) record).getAddress();
                  expireTime = Math.max(
                      (System.currentTimeMillis() + (((ARecord) record)
                          .getTTL() * 1000)), System.currentTimeMillis()
                          + MIN_TTL_VALUE);
                }
              }
            }

            if (address != null) {
              // LOG.info("Caching DNS Entry for Host:" + hostName);
              // update dns cache ...
              if (!noCache)
                _dnsCache.cacheIPAddressForHost(hostName, IPAddressUtils
                    .IPV4AddressToInteger(address.getAddress()), expireTime,
                    cname);
            }
          }
        } catch (TextParseException e) {
          resolverException = e;
        } catch (UnknownHostException e) {
          resolverException = e;
        }

        if (!retry) {
          // create result object ...
          result = new NIODNSQueryResult(this, client, hostName);

          if (response == null) {

            if (resolverException != null
                && (resolverException instanceof TextParseException || resolverException instanceof WireParseException)) {
              result.setStatus(Status.SERVER_FAILURE);
              result.setErrorDesc(StringUtils
                  .stringifyException(resolverException));
            } else {
              result.setStatus(Status.RESOLVER_FAILURE);
              if (resolverException != null) {
                if (_logger != null)
                  _logger.logDNSException(hostName, StringUtils
                      .stringifyException(resolverException));
                result.setErrorDesc(StringUtils
                    .stringifyException(resolverException));
              } else {
                if (_logger != null)
                  _logger.logDNSException(hostName, "Response was NULL");
              }
            }
          } else if (response.getRcode() != Rcode.NOERROR) {
            result.setStatus(Status.SERVER_FAILURE);
            result.setErrorDesc(Rcode.string(response.getRcode()));
            if (_logger != null)
              _logger
                  .logDNSFailure(hostName, Rcode.string(response.getRcode()));
          } else if (response.getRcode() == Rcode.NOERROR) {

            if (address != null) {

              result.setStatus(Status.SUCCESS);
              result.setAddress(address);
              result.setCName(cname);
              result.setTTL(expireTime);

              if (_logger != null) {
                _logger.logDNSQuery(hostName, address, expireTime, cname);
              }

            } else {
              result.setStatus(Status.SERVER_FAILURE);
              result.setErrorDesc("UNKNOWN-NO A RECORD");
              if (_logger != null)
                _logger.logDNSFailure(hostName, "NOERROR");
            }
          }
        }
      }
    } while (result == null);

    // if result is server failure ... and not via bad host cache ...
    if (!resultViaCache && result.getStatus() == Status.SERVER_FAILURE) {
      if (result.getErrorDescription().equals("NXDOMAIN")) {
        _badHostCache.cacheIPAddressForHost(hostName, 0, System
            .currentTimeMillis()
            + NXDOMAIN_FAIL_BAD_HOST_LIFETIME, null);
      }
    }

    // return result ... will be added to completion queue (to be retrieved via
    // poll method)...
    return result;
  }

  /**
   * DNSQuery - internal class used to encapsulate an active DNS Query Request
   * 
   */
  private static class DNSQuery implements Callable<NIODNSQueryResult> {

    private String              _hostName         = null;
    private NIODNSQueryClient   _client;
    private int                 _timeoutValue;
    private NIODNSLocalResolver _resolver;
    private int                 _flags;

    static final int     Flag_SkipCache    = 1 << 0;
    static final int     Flag_UseTCP       = 1 << 1;
    static final int     Flag_HighPriority = 1 << 2;

    /**
     * constructor
     * 
     * @param client
     *          - query client callback interface
     * @param hostName
     *          - host name to query for
     */
    public DNSQuery(NIODNSQueryClient client, NIODNSLocalResolver resolver,
        String hostName, int flags, int timeoutValue) {

      _hostName = hostName;
      _client = client;
      _flags = flags;
      _timeoutValue = timeoutValue;
      _resolver = resolver;
    }

    /**
     * 
     * overloaded from Callbale - primary work routine - called from worker
     * thread
     * 
     */
    public NIODNSQueryResult call() throws Exception {

      NIODNSQueryResult result = null;

      try {
        result = _resolver.doDNSQuery(_client, _hostName,
            (_flags & DNSQuery.Flag_UseTCP) != 0,
            (_flags & DNSQuery.Flag_SkipCache) != 0, _timeoutValue);
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw e;
      }
      return result;
    }

  };

  /**
   * queue a DNS resolution request
   * 
   * @param client
   *          - the callback interface
   * @param theHost
   *          - the host name to query for
   * @return
   * @throws IOException
   */
  public Future<NIODNSQueryResult> resolve(NIODNSQueryClient client,
      String theHost, boolean noCache, boolean highPriorityRequest,
      int timeoutValue) throws IOException {
    int flags = (noCache) ? DNSQuery.Flag_SkipCache : 0;
    if (_useTCP) {
      flags |= DNSQuery.Flag_UseTCP;
    }

    if (!highPriorityRequest || _highPriorityCompletionService == null)
      return _completionService.submit(new DNSQuery(client, this, theHost,
          flags, timeoutValue));
    else {
      return _highPriorityCompletionService.submit(new DNSQuery(client, this,
          theHost, flags, timeoutValue));
    }
  }

  /**
   * poll the resolver for any pending results .. will result in
   * NIODNSQueryClient interface calls for associate Query Requests.
   * 
   */
  public final void poll() {

    Future<NIODNSQueryResult> result;

    while ((result = _completionService.poll()) != null) {

      NIODNSQueryResult qResult = null;
      try {
        qResult = result.get();
      } catch (Exception e) {
        if (e instanceof IOException) {
          LOG.error(StringUtils.stringifyException(e));
        } else if (e instanceof RuntimeException) {
          LOG.fatal(StringUtils.stringifyException(e));
          e.printStackTrace();
          // die here ...
          System.exit(-1);
        }
      }
      if (qResult != null) {
        qResult.fireCallback();
      }
    }
  }
}
