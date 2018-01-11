package org.commoncrawl.io.internal;

import java.io.IOException;
import java.util.concurrent.Future;


public interface NIODNSResolver {

  /** queue a DNS resolution request 
   * 
   * @param client  - the callback interface 
   * @param theHost - the host name to query for  
   * @return
   * @throws IOException
   */
  public Future<NIODNSQueryResult> resolve(NIODNSQueryClient client, String theHost,boolean noCache,boolean highPriorityRequest,int timeoutValue) throws IOException; 
  
}
