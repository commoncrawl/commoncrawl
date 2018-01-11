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

import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.DomainNameUtils;
import org.commoncrawl.util.shared.IPAddressUtils;
import org.commoncrawl.util.shared.MovingAverage;
import org.junit.Test;

public class NIODNSCache {

  private static final Log LOG = LogFactory.getLog(NIODNSCache.class);

  
  private Node _root = new Node();
  private Node _ipRoot = new Node();
  private Stack<TreePosition> _iterationStack = new Stack<TreePosition>();
  private TreeSet<String> _cannonicalNames = new TreeSet<String>();
  private long numberOfNodes = 0;
  private long numberOfNodesRemoved = 0;
  private boolean enableIPAddressTracking = false;
  
  public  MovingAverage _dnsAddToCacheTime      = new MovingAverage(25);
  public  MovingAverage _dnsLookupFromCacheTime = new MovingAverage(25);

  
  private static final int searchMode_NodeChar = 1;
  private static final int searchMode_MultiNodeChar = 2;
  
  public NIODNSCache() { 
    _iterationStack.ensureCapacity(1024);
  }
  
  public synchronized long getActiveNodeCount() { 
    return numberOfNodes - numberOfNodesRemoved;
  }
  
  public synchronized void enableIPAddressTracking() { 
    enableIPAddressTracking = true;
  }
  
  public synchronized void clear() { 
    _root = new Node();
    _ipRoot = new Node();
    _cannonicalNames.clear();
    _iterationStack.clear();
    numberOfNodes = 0;
    numberOfNodesRemoved  = 0;
  }
  
  public static class DNSResult { 
    
    DNSResult(int ipAddress,long ipAddressTTL,String cName) { 
      _ipV4Address = ipAddress;
      _ttl = ipAddressTTL;
      _cName = cName;
    }
    
    public int getIPAddress() { 
      return _ipV4Address;
    }
    
    public long getTTL() { 
      return _ttl;
    }
    
    public String getCannonicalName() { 
      return _cName;
    }
    
    int       _ipV4Address;
    long    _ttl;
    String   _cName;
  }

  public synchronized DNSResult getIPAddressForHost(String hostName) { 
    
    long startTime = System.currentTimeMillis();
    
    DNSResult result = null;
    
    Node resolvedNode = findNode(hostName);
    String cName = null;
    int ipAddress = 0;
    long ttl = 0;
    
    if(resolvedNode != null) { 
      if (resolvedNode.getIPAddress() != 0) { 
        ipAddress = resolvedNode.getIPAddress();
        ttl = resolvedNode.getTimeToLive();
        cName = resolvedNode.getCannonicalName();
      }
      
      // if cannonical name is not null and our current address has expired ... 
      if (cName != null && ttl <= System.currentTimeMillis()) { 
        Node cannonicalNode = findNode(cName);
        
        // if cannonical node exists and it's ttl > our ttl 
        if (cannonicalNode != null && cannonicalNode.getTimeToLive() > resolvedNode.getTimeToLive()) { 
          
          ipAddress = cannonicalNode.getIPAddress();
          ttl       = cannonicalNode.getTimeToLive();

          resolvedNode.setIPAddress(ipAddress);
          resolvedNode.setTimeToLive(ttl);
        }
      }
    }
    
    if (ipAddress != 0) { 
      result = new DNSResult(ipAddress,ttl,cName);
    }
    
    long endTime = System.currentTimeMillis();
    
    _dnsLookupFromCacheTime.addSample((double)(endTime-startTime));
    
    return result;
  }
  
  
  private void addIPToNameNode(int ipAddress,String hostName) {
    String ipAddressStr = IPAddressUtils.IntegerToIPAddressString(ipAddress);;
    // create or access ip address node ... 
    Node ipAddressNode = addNode(_ipRoot, ipAddressStr);
    // increment hit count 
    ipAddressNode._expireTime++;
    // get previous cname for ip address node 
    String previousHostName = ipAddressNode._cannonicalName;
    
    if (previousHostName == null) { 
      setCannonicalNameForNode(ipAddressNode,hostName);
    }
    else { 
      // if not equal ....
      if (previousHostName.compareTo(hostName) != 0) { 
        // find common denominator ... 
        String previousTLDName = DomainNameUtils.extractRootDomainName(previousHostName);
        String currentTLDName  = DomainNameUtils.extractRootDomainName(hostName);
        
        // if tld name lengths match ...  
        if (previousTLDName != null && currentTLDName !=null && previousTLDName.length() == currentTLDName.length()) {
          
          // and if previous entry is NOT the TLD Name entry .. .
          if (previousTLDName.length() != previousHostName.length()) { 
            // ok time to do a comparison of tld names 
            if (previousTLDName.compareTo(currentTLDName) == 0) {
              
              // if new name is a tld name ... 
              if (currentTLDName.length() == hostName.length()) { 
                // use current tld name ...
                setCannonicalNameForNode(ipAddressNode,hostName);
              }
              else { 
                // now extract super domain name 
                String previousSubDomainNameParts[]  = previousHostName.substring(0,previousHostName.length() - previousTLDName.length()).split("\\.");
                String newSubDomainNameParts[]       = hostName.substring(0,hostName.length() - currentTLDName.length()).split("\\.");
                
                int partsToCompare = Math.min(previousSubDomainNameParts.length, newSubDomainNameParts.length);
                int i=0;
                for (i=0;i<partsToCompare;++i) { 
                  if (previousSubDomainNameParts[previousSubDomainNameParts.length - (i + 1)].compareTo(newSubDomainNameParts[newSubDomainNameParts.length - (i + 1)]) != 0) 
                      break;
                }
                if (i == 0) {
                  setCannonicalNameForNode(ipAddressNode,previousTLDName); 
                }
                else if (i < previousSubDomainNameParts.length) { 
                  StringBuffer builder = new StringBuffer();
                  for (int j=i;j<previousSubDomainNameParts.length;++j) { 
                    builder.append(previousSubDomainNameParts[j]);
                    builder.append(".");
                  }
                  builder.append(previousTLDName);
                  setCannonicalNameForNode(ipAddressNode,builder.toString());
                }
              }
            }
          }
        }
      }
    }
  }

  
  public synchronized Node cacheIPAddressForHost(String hostName,int ipAddress,long ttl,String cName) { 
    
    long startTime = System.currentTimeMillis();
    
    // first add the node to the list ... 
    Node node = addNameNode(hostName);
    node.setIPAddress(ipAddress);
    node.setTimeToLive(ttl);
    node.setLastTouchedTime(System.currentTimeMillis());
    
    if (cName != null) { 
      setCannonicalNameForNode(node,cName);
      
      Node cannonicalNameNode = addNameNode(cName);
      
      if (cannonicalNameNode != node && cannonicalNameNode.getTimeToLive() < node.getTimeToLive()) { 
        cannonicalNameNode.setIPAddress(ipAddress);
        cannonicalNameNode.setTimeToLive(ttl);
      }
    }
    
    if (enableIPAddressTracking) { 
      addIPToNameNode(ipAddress,(cName != null) ? cName : hostName);
    }
    
    long endTime = System.currentTimeMillis();
    
    _dnsAddToCacheTime.addSample((double)(endTime-startTime));
    
    return node;
  }
  
  private void setCannonicalNameForNode(Node node,String cName) { 
    SortedSet<String> subset = _cannonicalNames.subSet(cName, cName+"\0");
    if (!subset.isEmpty()) { 
      node._cannonicalName = subset.first();
    }
    else { 
      node._cannonicalName = cName;
      _cannonicalNames.add(cName);
    }
  }
  
  private static final class TreePosition { 
    
    TreePosition(Node node,int index) { 
      _node = node;
      _index = index;
    }
    
    public final Node getNode() { 
      return _node;
    }
    
    public final int getIndex() { 
      return _index;
    }
    
    public final void setIndex(int index) { 
      _index = index;
    }
    
    public final Node resolve() { 
      if (_index < _node.getChildCount()) { 
        return _node.getChildAt(_index);
      }
      return null;
    }
    
    private Node _node;
    private int    _index;
  }
  
  private synchronized void pruneCache() { 
    
    int nodesIterated = 0;
    
    // walk the tree ...
    if (_iterationStack.size() == 0) { 
      //System.out.println("Starting Iteration From Top");
      Node activeNode = (_root.getChildCount() != 0) ? _root.getChildAt(0) : null;
      if (activeNode != null) { 
        _iterationStack.add(new TreePosition(_root,0));
      }
    }
    else { 
      //System.out.println("Continuing Previous Iteration");
    }
    
    long startTime = System.currentTimeMillis();

    while (_iterationStack.size() != 0) { 
      
      TreePosition pos = _iterationStack.peek();
      // resolve the next child for the current node .. may return null...
      Node currentNode = pos.resolve();
      
      if (currentNode != null && currentNode.getChildCount() != 0) { 
        _iterationStack.push(new TreePosition(currentNode,0));
      }
      else {
        // if no more children for the current parent node, operate on the parent node itself ... 
        if (currentNode == null)  { 
          currentNode = pos.getNode();
          if (currentNode == _root)
            break;
          _iterationStack.pop();
          pos = _iterationStack.peek();
        }
        nodesIterated++;
        
        if (currentNode.getTimeToLive() <= startTime && currentNode.getChildCount() == 0) {
          // increment stats ... 
          numberOfNodesRemoved++;
          // remove child from parent ... 
          pos.getNode().removeChildAt(pos.getIndex());
          
          // check to see if time is up ... 
          if (System.currentTimeMillis() - startTime > 100) { 
            LOG.info("Time Up. breaking out. Nodes Iterated:" + nodesIterated);
            dumpStats();
            LOG.info("Prune Took:" + (System.currentTimeMillis() - startTime) + " MS" );            
            return;
          }
        }
        else { 
          pos.setIndex(pos.getIndex() + 1);
        }
        if (pos.getIndex() >= pos.getNode().getChildCount()) { 
          _iterationStack.pop();
          if (_iterationStack.size() != 0) { 
            // advance parent's index ... 
            TreePosition parentPos = _iterationStack.peek();
            parentPos.setIndex(parentPos.getIndex() + 1);
          }
        }
      }
    }
    
    long timeAfterPrune = System.currentTimeMillis();
    
    _iterationStack.removeAllElements();
    
    LOG.info("Prune Finished Iterating Whole Tree");
    dumpStats();
    LOG.info("Prune Took:" + (timeAfterPrune - startTime) + " MS" );
  }
  
  private synchronized final void dumpStats() { 
    LOG.info("Nodes Count:" + numberOfNodes);
    LOG.info("Nodes Removed:" + numberOfNodesRemoved);
  }
  
  
  static int numberOfNodesChildEQ1 =0;
  static int numberOfNodesChildLTEQ4 =0;
  static int numberOfNodesChildLTEQ8 =0;
  static int numberOfNodesChildGT8 =0;
  
  
  public static class Node { 

    private static final int GROWTH_FACTOR = 1;

    private char     _nodeChar;
    private char     _nodeCharArray[];
    private long     _expireTime =0;
    private int      _flags = 0;
    private int      _count = 0;  
    private Object   _children;
    private int      _ipAddress = 0;
    private String   _cannonicalName = null;
    private Node     _parent = null;
    private long     _lastTouched = -1;
    
    /*
    private Object _metadata = null;
    */
    
    public static final short Flag_Is_RootNode        = 1 << 0; 
    public static final short Flag_Is_TerminalNode   	= 1 << 1;
    public static final short Flag_Is_SuperNode  			= 1 << 2;
    public static final short Flag_Is_MultiCharNode  	= 1 << 3;
    public static final short Flag_NEXT_AVAILABLE_BIT_POS  = 4;
    
    private Node() {
      _parent = null;
      _flags = Flag_Is_RootNode;
    }
    
    public boolean isRootNode() { 
      return (_flags & Flag_Is_RootNode) != 0;
    }
    
    public Node getParentNode() { 
      return _parent;
    }
    
    public Node(Node parent,char nodeChar,int flags) {
      
      numberOfNodesChildEQ1++;
      
      _nodeChar = nodeChar;
      _flags = (short)flags;
      _parent = parent;
    }
    
    public final char getNodeChar() { 
      return _nodeChar;
    }
    
    public final char[] getMultiCharArray() { 
      return _nodeCharArray;
    }
    
    public final boolean isTerminalNode() { 
      return (_flags & Flag_Is_TerminalNode) != 0;
    }
    
    public final void markAsTerminalNode() { 
      _flags |= Flag_Is_TerminalNode;
    }
    
    public final boolean isSuperNode() { 
      return (_flags & Flag_Is_SuperNode) != 0;
    }
    
    public final void markAsSuperNode() { 
      _flags |=Flag_Is_SuperNode;
    }
    
    public final boolean isMultiCharNode() { 
      return (_flags & Flag_Is_MultiCharNode) != 0;
    }
    
    public final void setFlag(short flag) { 
    	_flags |= flag;
    }

    public final void clearFlag(short flag) { 
    	_flags &= ~flag;
    }
    
    public final boolean isFlagSet(short flag) { 
      return (_flags & flag) != 0;
    }
    
    public final long getTimeToLive() { 
      return _expireTime;
    }
    
    public final void setTimeToLive(long ttl) { 
      _expireTime = ttl;
    }
    
    /** update last touched time **/
    public final void setLastTouchedTime(long timeInMilliseconds) { 
      _lastTouched = timeInMilliseconds;
    }
   
    /** get last touched time **/
    public final long getLastTouchedTime() { 
      return _lastTouched;
    }
       
    
    public final void setIPAddress(int address) { 
      _ipAddress = address;
    }
    
    public final int getIPAddress() { 
      return _ipAddress;
    }
    
    public String getCannonicalName() { 
      return _cannonicalName;
    }

    public String getFullName() { 
      StringBuffer nameOut = new StringBuffer();
      
      Node currentNode = this;
      
      while (!currentNode.isRootNode()) { 
        if (currentNode.isMultiCharNode()) { 
          nameOut.append(currentNode.getMultiCharArray());
        }
        nameOut.append(currentNode.getNodeChar());
        
        currentNode = currentNode.getParentNode();
      }
      return nameOut.toString();
    }
    
    /*
    public final void setMetadata(Object metadata) { 
      _metadata = metadata;
    }
    
    public final Object getMetadata() { 
      return _metadata;
    }
    */

    public final int compareTo(char c) {
      if (_nodeChar < c)
        return -1;
      else if (_nodeChar > c)
        return 1;
      else
        return 0; 
    }
    
    public final int     getChildCount() { return _count; }
    public final Node    getChildAt(int index) { return (_count == 1) ? (Node)_children : ((Node[])_children)[index]; }
    public final void    removeChildAt(int index) {
      if (index >= _count)
        throw new RuntimeException("Invalid Index" );
      if (_count == 1) { 
        _children = null;
      }
      else { 
        int rightOfIndexCount = _count - (index + 1);
        if (rightOfIndexCount > 0) {
          if (_count > 2)
            System.arraycopy(_children, index + 1, _children, index, rightOfIndexCount);
          else 
            _children = ((Node[])_children)[1];
        }
        else { 
          if (_count == 2) { 
            _children = ((Node[])_children)[0];
          }
        }
      }
      _count--;
    }
    
    public final Node    findOrAddChild(NIODNSCache cacheObject,char nodeChar,boolean addChild) {

      int itemPosition= -1;
      
      if (_count == 1) { 
        if (((Node)_children).getNodeChar() == nodeChar) {
          itemPosition = 0;
        }
        else if (((Node)_children).getNodeChar() < nodeChar ) { 
          itemPosition = -2;
        }
      }
      else if (_count > 1) { 
        itemPosition = binarySearch((Node[])_children,0,_count,nodeChar);
      }
      
      // lt 0 means item was not found ... 
      if (itemPosition < 0 && addChild) { 

        // allocate a new Node
        Node newNode = new Node(this,nodeChar,0);
        // increment stats ... 
        cacheObject.numberOfNodes++;
        
        // normalize item positoin ... 
        itemPosition = Math.abs(itemPosition + 1);
        
        // check some bounding conditions.
        if (_count == 0) { 
          _children = newNode;
        }
        else { 
          // get some basic bounding information established ...  
          int leftCopyItems = itemPosition;
          int rightCopyItems = _count - itemPosition;
  
          Node copyArray[] = (_count == 1) ? null: (Node[]) _children;
          // if the array is full ... 
          if (_count ==1 || _count == ((Node[])_children).length) {
            if (_count == 1) { 
              numberOfNodesChildEQ1 --;
              numberOfNodesChildLTEQ4++;
            }
            else if (_count == 4) { 
              numberOfNodesChildLTEQ4 --;
              numberOfNodesChildLTEQ8++;
            }
            else if (_count == 8) { 
              numberOfNodesChildLTEQ8--;
              numberOfNodesChildGT8++;
            }
            // allocate a new array 
            int growAmount = ((_count / GROWTH_FACTOR) + 1) * GROWTH_FACTOR;
            copyArray = new Node[growAmount];
          }
        
          // copy lbound items only if copying into new array ... 
          if (leftCopyItems != 0 && copyArray != null) {   
            if (_count == 1)
              copyArray[0] = (Node)_children;
            else
              System.arraycopy(_children, 0, copyArray, 0, leftCopyItems);
          }
          // copy rbound items no matter what ... 
          if (rightCopyItems != 0) {
            if (_count == 1)
              copyArray[1] = (Node)_children;
            else
              System.arraycopy(_children, itemPosition, copyArray, itemPosition + 1, rightCopyItems);
          }
          
          // and assign copy array to children 
          _children = copyArray;
          // insert new item into array ... 
          ((Node[])_children)[itemPosition] = newNode;
        }
        // increment count 
        _count++;
      }
      if (itemPosition >=0) {
        return (_count == 1) ? (Node)_children : ((Node[])_children)[itemPosition];
      }
      return null;
    }
    
    
    // Like public version, but without range checks.
    private static int binarySearch(Node[] a, int fromIndex, int toIndex,char key) {
      
      int low = fromIndex;
      int high = toIndex - 1;

      while (low <= high) {
          int mid = (low + high) >>> 1;
          Node midVal = a[mid];
          
          int cmp = midVal.compareTo(key);
    
          if (cmp < 0)
            low = mid + 1;
          else if (cmp > 0)
            high = mid - 1;
          else
            return mid; // key found
      }
      return -(low + 1);  // key not found.
    }
    
    public void markAsMultiCharNode(char s[], int startOffset,int length) {
      if (length == 0) { 
        throw new RuntimeException();
      }
      _flags |= Flag_Is_MultiCharNode;
      _nodeCharArray = new char[length];
      System.arraycopy(s, startOffset, _nodeCharArray, 0, length);
    }
    
    public Node splitMultiCharNodeAt(NIODNSCache cacheObject,int splitIdx) {
    
      // allocate new intermediate node 
      Node newIntermediateNode = new Node(this,_nodeCharArray[splitIdx],0);
      
      
      //increment stats ... 
      cacheObject.numberOfNodes++;
      
      newIntermediateNode._ipAddress = this._ipAddress;
      // newOuterNode._metadata = this._metadata;
      newIntermediateNode._expireTime = this._expireTime;
      newIntermediateNode._cannonicalName = this._cannonicalName;
      
      this._ipAddress = 0;
      // this._metadata = null;
      newIntermediateNode._flags =(short) (this._flags & ~Flag_Is_MultiCharNode);
      this._flags = 0;
      this._cannonicalName = null;
      
      int leftOfSplitLength = splitIdx;
      int rightOfSplitLength = _nodeCharArray.length - splitIdx - 1;
      if (leftOfSplitLength != 0) { 
        newIntermediateNode.markAsMultiCharNode(_nodeCharArray,0,leftOfSplitLength);
      }
      if (rightOfSplitLength != 0) { 
        char array[] = new char[rightOfSplitLength];
        System.arraycopy(_nodeCharArray,splitIdx+1 , array, 0, rightOfSplitLength);
        _nodeCharArray = array;
        _flags |= Flag_Is_MultiCharNode;
      }
      else { 
        _nodeCharArray = null;
        _flags &= ~Flag_Is_MultiCharNode;
      }
      
      // absorb the old node's children ... 
      newIntermediateNode._children = this._children;
      newIntermediateNode._count    = this._count;
      
      // and re-parent our children to point to new intermediate branch node 
      for (int i=0;i<newIntermediateNode.getChildCount();++i) { 
        newIntermediateNode.getChildAt(i)._parent = newIntermediateNode;
      }
      
      // make the new node the old node's only child ... 
      this._children         = newIntermediateNode;
      this._count             = 1;
      
      return newIntermediateNode;
    }
    
  }

  /** add a name node **/
  public Node addNameNode(String nodeName) {
    return addNode(_root,nodeName);
  }
  
  /** underlying routine shared by name and ip lookup routines **/
  private Node addNode(Node rootNode,String path){
    // search super node in reverse order ... 
    String s = path.toLowerCase();
    Node node = rootNode;
    
    if (s.length() > 0) {
      
      int multiNodeIdx = -1;
      int searchMode = searchMode_NodeChar;
      
      for (int i= s.length()-1; i >= 0; i--) {
        if (searchMode == searchMode_NodeChar) {
          // find the node for the next character ... 
          node= node.findOrAddChild(this,s.charAt(i),true);
          // update/set the node's ttl
          //node.setTimeToLive(Math.max(node.getTimeToLive(),ttl));
          // if the returned node is a multi-char node, search into the node ... 
          if (node.isMultiCharNode()) {
            searchMode = searchMode_MultiNodeChar;
            multiNodeIdx = node.getMultiCharArray().length - 1;
          }
          // otherwise... if the node has no children and the index is not zero and current char is not '.' token ... 
          else if (node.getChildCount() == 0 && i != 0 && s.charAt(i) != '.' && !node.isTerminalNode()) { 
            int multiNodeScanStart = i-1;
            int multiNodeCharEndPos = multiNodeScanStart;
            // walk backwards until either end of string is reached, or a '.' token is located ... 
            while (multiNodeCharEndPos >= 0 && s.charAt(multiNodeCharEndPos) != '.')
              multiNodeCharEndPos--;
            
            // if we actually accumulated something in the scan buffer ... 
            if (multiNodeScanStart - multiNodeCharEndPos != 0) { 
              // if scan terminated before the end of the string, then a '.' token terminated the scan ... 
              if (multiNodeCharEndPos != -1) {
                // gobble up the appropriate number of characters ... 
                node.markAsMultiCharNode(s.toCharArray(),multiNodeCharEndPos + 1,i-(multiNodeCharEndPos + 1));
                //set up i so that loop can continue ... 
                i = multiNodeCharEndPos + 1;
                // and break out of multi-char scan mode ... 
                searchMode = searchMode_NodeChar;
              }
              else { 
                // gobble up remaining characters and covert node to multi-char node ...  
                node.markAsMultiCharNode(s.toCharArray(),0,i);
                // and break out of loop... 
                break;
              }
            }
          }
        }
        else { 
          if (multiNodeIdx ==-1 || i == -1) { 
            throw new RuntimeException();
          }
          if (node.getMultiCharArray()[multiNodeIdx] != s.charAt(i)) { 
            node.splitMultiCharNodeAt(this,multiNodeIdx);
            i+=1;
            searchMode = searchMode_NodeChar;
          }
          else { 
            multiNodeIdx -= 1;
            if (multiNodeIdx < 0)
              searchMode = searchMode_NodeChar;
          }
        }
        
      }
      // finally, if we are still in multi-char search mode and multiNodeIdx != -1
      // this means that we terminated a successfull match int the MIDDLE of a multi-char node
      if (searchMode == searchMode_MultiNodeChar && multiNodeIdx != -1) { 
        // in this case, we have to split the multi-char node appropriately ... 
        node.splitMultiCharNodeAt(this,multiNodeIdx);
      }
      node.markAsTerminalNode();
      
      return node;
    }
    return null;
  }
  
  
  public Node findNode(String nodeName) { 
    Node nodeOut = _findNode(_root,nodeName);
    if (nodeOut != null && nodeOut.isTerminalNode()) { 
      nodeOut.setLastTouchedTime(System.currentTimeMillis());
    }
    return nodeOut;
  }
  
  private synchronized Node _findNode(Node rootNode,String nodeName) { 
    
    String s = nodeName.toLowerCase();
    
    Node node= rootNode;
    
    if (s.length() > 0) {
      
      Node lastSubTerminalNode = null;
      
      for (int i= s.length()-1; i >= 0 && node != null; i--) {
        
        if (s.charAt(i) == '.' && node != _root) {
          lastSubTerminalNode = node;
        }
        
        node= node.findOrAddChild(this,s.charAt(i),false);
        
        // now if this is a multi-char node ... 
        if (node != null && node.isMultiCharNode()) { 
          int innerScanPos = i-1;
          int multiCharArrayScanPos = node.getMultiCharArray().length -1;
          while (innerScanPos >= 0 &&  multiCharArrayScanPos >=0) { 
            if (s.charAt(innerScanPos) == node.getMultiCharArray()[multiCharArrayScanPos]) { 
              innerScanPos--;
              multiCharArrayScanPos--;
            }
            else { 
              break;
            }
          }
          
          // now first condition for a successfull match is than we completely scanned the mutli-char array ... 
          if (multiCharArrayScanPos == -1) { 
            // now if char scan also reached the beginning of the search string ... 
            if (innerScanPos == -1) { 
              // this is a true match ... 
              return (node.isTerminalNode()) ? node : null;
            }
            // otherwise ... check the character at new position ... 
            else { 
              // set i to inner scan pos + 1 (so that out loop will properly adjust value)
              i=innerScanPos+1;
            }
          }
          else { 
            node = null;
            break;
          }
        }
      }
      
      if (node == null && lastSubTerminalNode != null) {
        if (lastSubTerminalNode.isSuperNode()) {
          node = lastSubTerminalNode;
        }
      }
      if (node != null)
        return (node.isTerminalNode()) ? node : null;
    }
    return null;
  }
  
  
  
  @Test
  public void simpleTest() throws Exception {
    int ipAddress =IPAddressUtils.IPV4AddressStrToInteger("68.178.211.35");
    cacheIPAddressForHost("www.matlockpark.com",ipAddress, 1226959057707L, "matlockpark.com");
    DNSResult result = getIPAddressForHost("www.matlockpark.com");
    org.junit.Assert.assertTrue(result.getCannonicalName().equals("matlockpark.com"));
    org.junit.Assert.assertTrue(result.getIPAddress() == ipAddress);
    org.junit.Assert.assertTrue(result.getTTL() == 1226959057707L);
  }
  
  public static interface LoadFilter {
    String  validateName(String hostName);
    boolean loadItem(String hostName,String ipAddress,String cName,long expireTime,long lastTouchedTime);
  }
  
  
  public synchronized  void loadTree(InputStream inputStream,LoadFilter loadFilter) throws IOException { 
    
    int lineCount = 0;
    
    clear();
    
    BufferedReader lineReader = new BufferedReader(new InputStreamReader(inputStream),1024000);

    String line = null;
    
    while ((line = lineReader.readLine()) != null) { 
      try {
        
        String elements[] = line.split(",");
        
        if (elements.length >= 4) { 

          String hostName = elements[0];
          String ipAddress = elements[1].substring(1);
          long ttl = Long.parseLong(elements[2]);
          String cname      = null;
          if (!elements[3].equals("null")) { 
            cname = elements[3];
          }
          long lastTouched = -1;
          if (elements.length > 5) { 
            lastTouched = Long.parseLong(elements[4]);
          }

          int ipAddressInteger = IPAddressUtils.IPV4AddressStrToInteger(ipAddress);
          if (loadFilter != null) { 
            hostName = loadFilter.validateName(hostName);
          }
          if (loadFilter == null || loadFilter.loadItem(hostName, ipAddress, cname, ttl,lastTouched)) { 
            cacheIPAddressForHost(hostName, ipAddressInteger, ttl, cname);
          }
          
        }
      }
      catch (Exception e) { 
        LOG.error(CCStringUtils.stringifyException(e));
      }
      
      if (++lineCount % 100000 == 0)
        LOG.info("Processed " + lineCount + " lines");
    }
    
    LOG.info("Processed a total of:" + lineCount + " lines");
  }
  
  public static interface NodeDumpFilter { 
    boolean dumpTerminalNode(Node node);
  }
  
  public synchronized void dumpNameTree(OutputStream outputStream,NodeDumpFilter filter)throws IOException {
    
    PrintWriter printWriter = new PrintWriter(new BufferedOutputStream(outputStream));
    dumpNode(printWriter,_root,filter);
  }
  
  public synchronized void dumpIPAddressTree(OutputStream outputStream)throws IOException {
    
    PrintWriter printWriter = new PrintWriter(new BufferedOutputStream(outputStream));
    dumpIPNode(printWriter,_ipRoot);
  }
  
  
   
  private synchronized void collectTerminalNodes(Node node,List<Node> terminalNodeVector)throws IOException {
    if (node.isTerminalNode()) {
      terminalNodeVector.add(node);
    }
    for (int i=0;i<node.getChildCount();++i) { 
      collectTerminalNodes(node.getChildAt(i),terminalNodeVector);
    }
  }
  
  public synchronized void collectTerminalIPNodes(List<Node> terminalNodeVector){
    try { 
      collectTerminalNodes(_ipRoot,terminalNodeVector);
    }
    catch (IOException e) { 
      
    }
  } 

  public synchronized void collectTerminalNodes(List<Node> terminalNodeVector){
    try { 
      collectTerminalNodes(_root,terminalNodeVector);
    }
    catch (IOException e) { 
      
    }
  } 
  
  
  void dumpNode(PrintWriter printWriter,Node node,NodeDumpFilter filter)throws IOException { 
    if (node.isTerminalNode()) {
      
      if (filter == null || filter.dumpTerminalNode(node)) { 
        printWriter.append(node.getFullName());
        printWriter.append(",");
        try {
          printWriter.append((IPAddressUtils.IntegerToInetAddress(node.getIPAddress()).toString()));
        } catch (UnknownHostException e) {
          LOG.error(CCStringUtils.stringifyException(e));
        }
        printWriter.append(",");
        printWriter.append(Long.toString(node.getTimeToLive()));
        printWriter.append(",");
        if (node._cannonicalName != null) { 
          printWriter.append(node._cannonicalName);
        }
        else { 
          printWriter.append("null");
        }
        printWriter.append(",");
        printWriter.append(Long.toString(node.getLastTouchedTime()));
        
        printWriter.append("\n");
      }
    }
    
    for (int i=0;i<node.getChildCount();++i) { 
      dumpNode(printWriter,node.getChildAt(i),filter);
    }
  }
  

  void dumpIPNode(PrintWriter printWriter,Node node)throws IOException { 
    if (node.isTerminalNode()) {
      printWriter.append(node.getFullName());
      printWriter.append(",");
      printWriter.append(node.getCannonicalName());
      printWriter.append("\n");
    }
    
    for (int i=0;i<node.getChildCount();++i) { 
      dumpIPNode(printWriter,node.getChildAt(i));
    }
  }

  @Test
  public void validateDumpCode() throws Exception {
    addNameNode("www.feeds.feedburner.com");
    addNameNode("pictures.google.com");
    addNameNode("pictures2.google.com");
    addNameNode("gmail.google.com");
    addNameNode("foobar.google.com");

    assertTrue(findNode("feeds.feedburner.com") == null);
    assertTrue(findNode("www.feeds.feedburner.com") != null);
    assertTrue(findNode("pictures.google.com") != null);
    assertTrue(findNode("pictures2.google.com") != null);
    assertTrue(findNode("gmail.google.com") != null);
    assertTrue(findNode("foobar.google.com") != null);


    
    dumpNameTree(System.out,null);
  }
  
  @Test
  public void validateTrieCode() throws Exception {
    
//    addNode("www.google.com",System.currentTimeMillis() + 60000).setMetadata("www.google.com");
//    addNode("pictures.google.com",System.currentTimeMillis() + 60000).setMetadata("pictures.google.com");
//    addNode("pictures2.google.com",System.currentTimeMillis() + 60000).setMetadata("pictures2.google.com");
//    addNode("gmail.google.com",System.currentTimeMillis() + 60000).setMetadata("gmail.google.com");
//    addNode("foobar.google.com",System.currentTimeMillis() + 60000).setMetadata("foobar.google.com");
//    Node googleSuperNode = addNode("google.com",System.currentTimeMillis() + 60000);
//    googleSuperNode.markAsSuperHost();
//    googleSuperNode.setMetadata("google.com");
    
//   Node wwwGoogleCom = findNode("www.google.com");
//   Node productsGoogleCom = findNode("products.google.com");
   
//   org.junit.Assert.assertTrue(wwwGoogleCom.getMetadata().equals("www.google.com"));
//   org.junit.Assert.assertTrue(productsGoogleCom.getMetadata().equals("google.com"));
   
   URL resourceURL = ClassLoader.getSystemResource("urls.txt");
   
   if (resourceURL == null) {
     throw new FileNotFoundException();
   }
   
   Set<String> hostSet = new HashSet<String>();
   
   for (int pass=0;pass<1;++pass) { 
     
     System.out.println("running pass:" + pass);
     
     InputStream stream = resourceURL.openStream();
     
     BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
     
     String line;
     int lineCount = 0;
     
     while ((line = reader.readLine()) != null) { 
       try { 
         URL url = new URL(line);
         if (url.getHost() != null && url.getHost().length() != 0) {
             if (pass == 0) { 
               //hostSet.add(url.getHost().toLowerCase());
               cacheIPAddressForHost(url.getHost(), url.getHost().hashCode(), System.currentTimeMillis() + 5000 + (int)(Math.random() * 30000.00),Integer.toString(url.getHost().hashCode()));
             }
             else {
               
               Node node = findNode(url.getHost());
               if (node != null) { 
                 
                 if (node.getIPAddress() != url.getHost().hashCode()) { 
                   throw new RuntimeException("Metadata Mismatch for host:" +url.getHost() +".Excpected:" + url.getHost().hashCode() + " Got: " + node.getIPAddress());
                 }
               }
               else { 
                 throw new RuntimeException("Node Null! Excpected:" + url.getHost());
               }
             }
         }
         if (++lineCount % 10000 == 0) {
           System.out.println("pruning mid-stream");
           pruneCache();
           System.out.println("sleeping a little while ..");
           Thread.sleep(100);
           System.out.println("wokeup");
           //System.out.print(".");
         }
       }
       catch (MalformedURLException e) { 
         
       }
       catch (Exception e) { 
         e.printStackTrace();
       }
     }
     System.out.println("done reading entries");
   }
   System.out.println("Host Count:" + hostSet.size());
   System.out.println("Number of Nodes:" + numberOfNodes);
   System.out.println("Number of Nodes EQ_1:" + numberOfNodesChildEQ1);
   System.out.println("Number of Nodes LTEQ_4:" + numberOfNodesChildLTEQ4);
   System.out.println("Number of Nodes LTEQ_8:" + numberOfNodesChildLTEQ8);
   System.out.println("Number of Nodes GT_8:" + numberOfNodesChildGT8);
   
   
   while (true) { 
     System.out.println("sleeping...");
     Thread.sleep(100);
     System.out.println("pruning.. ");
     pruneCache();
   }
   
  }
  
  public static void main(String[] args) {
    
    File file = new File(args[0]);
    InputStream stream;

    NIODNSCache dnsCache = new NIODNSCache();

    try {
      stream = new FileInputStream(file);
      
      
      long timeStart = System.currentTimeMillis();
      LOG.info("Loading Tree from Stream");
      dnsCache.loadTree(stream,null);
      long timeEnd = System.currentTimeMillis();
      LOG.info("Load took:" + (timeEnd - timeStart) + " NodeCount:" + dnsCache.getActiveNodeCount());

      LOG.info("Pruning cache based on filter (TTL)");
      timeStart = System.currentTimeMillis();
      ByteArrayOutputStream streamOut = new ByteArrayOutputStream(1024*1024*100);
      dnsCache.dumpNameTree(streamOut, new NIODNSCache.NodeDumpFilter() {

        @Override
        public boolean dumpTerminalNode(Node node) {
          if (node.getTimeToLive() >= System.currentTimeMillis()){ 
            return true;
          }
          return false;
        } 
      });
      timeEnd = System.currentTimeMillis();
      LOG.info("Dump took:" + (timeEnd- timeStart));
      
      LOG.info("Reloading cache from stream");
      timeStart = System.currentTimeMillis();
      dnsCache = new NIODNSCache();
      dnsCache.loadTree(new ByteArrayInputStream(streamOut.toByteArray()),null);
      timeEnd = System.currentTimeMillis();
      LOG.info("Reload took:" + (timeEnd - timeStart) + "NodeCount:" + dnsCache.getActiveNodeCount());
      
      /*
      
      LOG.info("Collecting terminal IP nodes");
      List<Node> terminalIPNodes = new Vector<Node>();
      dnsCache.collectTerminalNodes(dnsCache._ipRoot, terminalIPNodes);
      LOG.info("Sorting nodes");
      Collections.sort(terminalIPNodes,new Comparator<NIODNSCache.Node>() {

        @Override
        public int compare(Node o1, Node o2) {
          return o1.getTimeToLive() > o2.getTimeToLive() ? -1 : o1.getTimeToLive() < o2.getTimeToLive() ? 1 : 0; 
        } 
      });
      LOG.info("Top 1000 Hot Nodes");
      int maxNodes = Math.min(1000, terminalIPNodes.size());
      for (int i=0;i<maxNodes;++i) {
        LOG.info("Node:" + terminalIPNodes.get(i).getFullName() + " CName:" + terminalIPNodes.get(i).getCannonicalName() + " HitCount:" + terminalIPNodes.get(i).getTimeToLive());
      }
      */
      
      /*
      LOG.info("Dumping IP Tree to stdout");
      dnsCache.dumpIPAddressTree(System.out);
      
      LOG.info("Dumping Name Tree to stdout");
      dnsCache.dumpNameTree(System.out);
      */
    } catch (IOException e) {
      e.printStackTrace();
   }
  }
}
