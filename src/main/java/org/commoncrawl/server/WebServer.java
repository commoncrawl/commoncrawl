package org.commoncrawl.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Semaphore;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.HttpServer.StackServlet;
import org.commoncrawl.util.shared.CCStringUtils; //import org.apache.hadoop.mapred.StatusHttpServer;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;

// Bulk of this class is copied from
// {@link org.apache.hadoop.hbase.InfoServer}.  

/**
 * Create a Jetty embedded server to answer http requests. The primary goal is
 * to serve up status information for the server. There are three contexts:
 * "/stacks/" -> points to stack trace "/static/" -> points to common static
 * files (src/webapps/static) "/" -> the jsp server code from
 * (src/webapps/<name>)
 */
public class WebServer {
  // Bulk of this class is copied from
  // {@link org.apache.hadoop.mapred.StatusHttpServer}. StatusHttpServer
  // is not amenable to subclassing. It keeps webAppContext inaccessible
  // and will find webapps only in the jar the class StatusHttpServer was
  // loaded from.
  private static final Log         LOG            = LogFactory
                                                      .getLog(WebServer.class
                                                          .getName());
  private org.mortbay.jetty.Server webServer;
  private Connector                listener;
  private boolean                  findPort;
  private Context                  webAppContext;
  private CommonCrawlServer        _hostServer;
  private boolean                  _asyncDispatch = false;

  /**
   * get access to the web app context
   * 
   * @return Context object
   */
  public Context getWebAppContext() {
    return webAppContext;
  }

  public static class AsyncWebApplicationContext extends WebAppContext {

    WebServer _server = null;

    public AsyncWebApplicationContext(WebServer webServer) {
      super();
      _server = webServer;
    }

    @Override
    public void handle(final String pathInContext,
        final HttpServletRequest request, final HttpServletResponse response,
        final int dispatch) throws IOException, ServletException {
      LOG.info("Received Web Request for Path:" + pathInContext);
      // use async dispatch mechanism ...
      if (pathInContext.endsWith(".jsp")) {

        // LOG.info("Scheduling Async Web Request for Path:" + pathInContext);
        // allocate async web request object ..
        AsyncWebServerRequest asyncWebRequest = new AsyncWebServerRequest("",
            null) {

          @Override
          public boolean handleRequest(Semaphore completionSemaphore)
              throws IOException {
            // LOG.info("Executing Async Web Request for Path:" +
            // pathInContext);
            try {
              AsyncWebApplicationContext.super.handle(pathInContext, request,
                  response, dispatch);
            } catch (ServletException e) {
              LOG.error(CCStringUtils.stringifyException(e));
              throw new IOException(e);
            }
            // LOG.info("Done Executing Async Web Request for Path:" +
            // pathInContext);
            return false;
          }

        };
        // and dispatch it using the server's event loop
        asyncWebRequest.dispatch(_server._hostServer.getEventLoop());
        // LOG.info("Returned from Async Web Request Excecution for Path:" +
        // pathInContext);

        // now check of exceptions ...
        if (asyncWebRequest.getException() != null) {
          // re-raise the exception in the web-server's thread/
          throw asyncWebRequest.getException();
        }
      } else {
        super.handle(pathInContext, request, response, dispatch);
      }
    }
  }

  /**
   * Create a required listener for the Jetty instance listening on the port
   * provided. This wrapper and all subclasses must create at least one
   * listener.
   */
  protected Connector createBaseListener() throws IOException {
    SelectChannelConnector ret = new SelectChannelConnector();
    ret.setLowResourceMaxIdleTime(10000);
    ret.setLowResourceMaxIdleTime(-1);
    ret.setAcceptQueueSize(500);
    ret.setResolveNames(false);
    ret.setUseDirectBuffers(false);
    ret.setAcceptors(4);
    return ret;
  }

  final String[] ALL_URLS = { "/*" };

  /**
   * Create a status server on the given port. The jsp scripts are taken from
   * src/webapps/<code>name<code>.
   * 
   * @param name
   *          The name of the server
   * @param port
   *          The port to use on the server
   * @param findPort
   *          whether the server should start at the given port and increment by
   *          1 until it finds a free port.
   */
  public WebServer(CommonCrawlServer hostServer, String bindAddress, int port,
      boolean findPort, boolean useAsyncDispatch) throws IOException {

    final WebServer theWebServer = this;
    this._hostServer = hostServer;
    this._asyncDispatch = useAsyncDispatch;
    this.webServer = new Server();
    this.webServer.setThreadPool(new QueuedThreadPool());
    this.findPort = findPort;
    this.listener = createBaseListener();
    this.listener.setPort(port);
    this.listener.setHost(bindAddress);
    this.webServer.addConnector(listener);

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    webServer.setHandler(contexts);

    // add default WebAppContext
    // WebAppContext =

    // Set up the context for "/logs/" if "commoncrawl.log.dir" property is
    // defined.
    String logDir = System.getProperty("commoncrawl.log.dir");

    // set up the context for "/" jsp files
    File webappDir = null;

    if (hostServer.getAppName() != null) {
      webappDir = getWebAppsPath();

      LOG.info("WebApps Dir is:" + webappDir);

      if (useAsyncDispatch) {
        this.webAppContext = new AsyncWebApplicationContext(this);
      } else {
        this.webAppContext = new WebAppContext();
      }

      // add it to the gloabl list of contexts
      contexts.addHandler(webAppContext);

      webAppContext.setContextPath("/");

      ((WebAppContext) this.webAppContext).setWar(webappDir.getAbsolutePath());

      // if (webAppURL != null) {
      // set up the context for "/static/*"
      File webAppStaticDir = new File(webappDir, "/static");

      File files[] = webAppStaticDir.listFiles();
      boolean hasStaticFilesInRoot = false;
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            Context staticContext = new Context(contexts, "/" + file.getName());
            staticContext.setResourceBase(file.toURI().toString());
            staticContext.addServlet(DefaultServlet.class, "/");
          } else {
            hasStaticFilesInRoot = true;
          }
        }
      }

      if (hasStaticFilesInRoot) {
        Context staticContext = new Context(contexts, "/");
        staticContext.setResourceBase(webAppStaticDir.toURI().toString());
        staticContext.addServlet(DefaultServlet.class, "/");
      }
    }
    /*
     * } else { webAppContext = new Context();
     * webAppContext.setContextPath("/"); }
     */
    // SKIP THIS since we already added webappcontext to global context list
    // webServer.addHandler(webAppContext);
    if (logDir != null) {

      Context logContext = new Context(contexts, "/logs");
      logContext.setResourceBase(logDir);
      logContext.addServlet(DefaultServlet.class, "/");
    }

    addServlet("stacks", "/stacks", StackServlet.class);

  }

  protected void defineFilter(Context ctx, String name, String classname,
      Map<String, String> parameters, String[] urls) {

    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    holder.setInitParameters(parameters);
    FilterMapping fmap = new FilterMapping();
    fmap.setPathSpecs(urls);
    fmap.setDispatches(Handler.ALL);
    fmap.setFilterName(name);
    ServletHandler handler = ctx.getServletHandler();
    handler.addFilter(holder, fmap);
  }

  public org.mortbay.jetty.Server getServer() {
    return webServer;
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * 
   * @param name
   *          The name of the attribute
   * @param value
   *          The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    this.webAppContext.setAttribute(name, value);
  }

  /**
   * Add a servlet in the server.
   * 
   * @param name
   *          The name of the servlet (can be passed as null)
   * @param pathSpec
   *          The path spec for the servlet
   * @param servletClass
   *          The servlet class
   */
  public <T extends HttpServlet> ServletHolder addServlet(String name,
      String pathSpec, Class<T> servletClass) {
    ServletHolder holder = new ServletHolder(servletClass);
    if (name != null) {
      holder.setName(name);
    }
    webAppContext.addServlet(holder, pathSpec);

    return holder;
  }

  private static RuntimeException makeRuntimeException(String msg,
      Throwable cause) {
    RuntimeException result = new RuntimeException(msg);
    if (cause != null) {
      result.initCause(cause);
    }
    return result;
  }

  /**
   * Get the value in the webapp context.
   * 
   * @param name
   *          The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return this.webAppContext.getAttribute(name);
  }

  /**
   * Get the pathname to the <code>webapps</code> files.
   * 
   * @return the pathname as a URL
   */
  public File getWebAppsPath() throws IOException {
    String webAppName = _hostServer.getAppName();

    URL url = WebServer.class.getClassLoader().getResource(webAppName);
    if (url == null)
      throw new IOException("webapps path not found in CLASSPATH");
    
    try {
      return new File(url.toURI());
    } catch (URISyntaxException e) {
      LOG.error(CCStringUtils.stringifyException(e));
      throw new IOException(e);
    }
  }

  /**
   * Get the port that the server is on
   * 
   * @return the port
   */
  public int getPort() {
    return this.listener.getPort();
  }

  public void setThreads(int min, int max, int low) {
    ((QueuedThreadPool) this.webServer.getThreadPool()).setMinThreads(min);
    ((QueuedThreadPool) this.webServer.getThreadPool()).setMaxThreads(max);
    ((QueuedThreadPool) this.webServer.getThreadPool()).setLowThreads(low);
  }

  public void setLowResourceTimeout(int milliseconds) {
    ((QueuedThreadPool) this.webServer.getThreadPool())
        .setMaxIdleTimeMs((int) milliseconds);
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      while (true) {
        try {
          this.webServer.start();
          break;
        } catch (org.mortbay.util.MultiException ex) {
          LOG.error(CCStringUtils.stringifyException(ex));
          throw ex;
        }
      }
    } catch (IOException ie) {
      throw ie;
    } catch (Exception e) {
      IOException ie = new IOException("Problem starting http server");
      ie.initCause(e);
      throw ie;
    }
  }

  /**
   * stop the server
   */
  public void stop() throws InterruptedException {
    try {
      this.webServer.stop();
    } catch (Exception e) {
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }

  /**
   * get the web app context
   * 
   */
  public ContextHandlerCollection getContextHandlerCollection() {
    return (ContextHandlerCollection) webServer.getHandler();
  }
}