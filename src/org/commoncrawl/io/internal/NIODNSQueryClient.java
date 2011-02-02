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

import java.net.InetAddress;
import java.util.concurrent.FutureTask;


/**
 * NIODNSQueryClient - status interface for NIODNSResolver 
 * 
 * @author rana
 *
 */
public interface NIODNSQueryClient {

    public enum Status { SUCCESS ,SERVER_FAILURE, RESOLVER_FAILURE };
	
	
  /** address resolution failed on the specified host */
  void AddressResolutionFailure(NIODNSResolver source,String hostName,Status status,String errorDesc);
  
  /** address resolution succeeded for the specified host
   * 
   * @param hostName - the host name associated with the query results 
   * @param cName    - the cannonical name of the specified host name (as returned by DNS)
   * @param address  - the IP address for the specified host 
   */
  void AddressResolutionSuccess(NIODNSResolver source,String hostName,String cName, InetAddress address,long addressTTL);
  
  void DNSResultsAvailable();
  
  /** task competed **/
  void done(NIODNSResolver source,FutureTask<NIODNSQueryResult> task);
  
}
