package org.commoncrawl.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.async.EventLoop;
import org.commoncrawl.async.Timer;
import org.commoncrawl.util.shared.CCStringUtils;

public abstract class AsyncWebServerRequest {

  /** logging **/
  private static final Log LOG          = LogFactory
                                            .getLog(AsyncWebServerRequest.class);

  final String             requestName;
  final Writer             writer;
  PrintWriter              out          = null;

  public IOException       exceptionOut = null;

  public AsyncWebServerRequest(String requestName, Writer incomingWriter) {
    this.requestName = requestName;
    this.writer = incomingWriter;
    if (incomingWriter != null) {
      this.out = new PrintWriter(incomingWriter);
    }
  }

  public AsyncWebServerRequest(String requestName) {
    this.requestName = requestName;
    this.out = null;
    this.writer = null;
  }

  public IOException getException() {
    return exceptionOut;
  }

  public abstract boolean handleRequest(Semaphore completionSemaphore)
      throws IOException;

  public void dispatch(EventLoop eventLoop) {

    final Semaphore waitState = new Semaphore(0);

    eventLoop.setTimer(new Timer(0, false, new Timer.Callback() {

      public void timerFired(Timer timer) {
        boolean isClosure = false;
        try {
          isClosure = handleRequest(waitState);
        } catch (IOException exception) {
          exceptionOut = exception;
          if (out != null) {
            ((PrintWriter) out).print("<pre>WebRequest:" + requestName
                + " Failed with exception: "
                + CCStringUtils.stringifyException(exception) + " </pre>");
          }
        } finally {
          if (!isClosure) {
            waitState.release();
          }
        }
      }
    }));

    waitState.acquireUninterruptibly();
  }
}
