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

package org.commoncrawl.rpc.base.shared;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public abstract class RPCStruct implements Cloneable{

	static protected final String emptyString = new String();
	
	/** clone support **/
	public abstract Object clone() throws CloneNotSupportedException;
	
	/** hash code support **/
	public abstract int hashCode();
	
	/** equals support **/
	public abstract boolean equals(final Object peer);

	/** merge support *
	 * @throws CloneNotSupportedException */
	public abstract void merge(Object peer) throws CloneNotSupportedException;

	 /**
	   * Serialize to the given output stream using the given protocol  encoder
	   * @param out output stream
	   * @param encoder protocol encoder
	   */
	abstract public void serialize(DataOutput out, BinaryProtocol encoder) throws IOException;
	  
	  /**
	   * Deserialize a message from the given stream using the given protocol decoder 
	   * @param in input stream
	   * @param decoder protocol decoder
	   */
	abstract public void deserialize(DataInput in, BinaryProtocol decoder) throws IOException;
	
		
	/**
	 *  return the struct's key as a string value 
	 *  **/
	public String getKey() { return null; }
	 
}
