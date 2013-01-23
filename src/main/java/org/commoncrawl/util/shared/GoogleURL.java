package org.commoncrawl.util.shared;



/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

/** 
 * A (JNI) wrapper around to Google URL Canonicalization Library
 * @author rana
 *
 */
public class GoogleURL {

  
  static public final String emptyString = "";
  static {
      // Ensure native JNI library is loaded
      System.loadLibrary("GoogleURL_jni");
      internal_init(GoogleURLComponent.class);
  }
  
  private native void initializeFromURL(String urlStirng);
  @SuppressWarnings("unchecked")
  private native static void internal_init(Class componentClass);
  
  public GoogleURL(String urlString) { 
    initializeFromURL(urlString);
  }
  
  // Identifies different components.
  enum ComponentType {
    SCHEME,
    USERNAME,
    PASSWORD,
    HOST,
    PORT,
    PATH,
    QUERY,
    REF,
  }

  public boolean isValid() { 
    return _isValid; 
  }
  
  public boolean has_scheme() {
    return _scheme.len >= 0;
  }
  
  public boolean has_username() {
    return _userName.len >= 0;
  }
  
  public boolean has_password() {
    return _password.len >= 0;
  }
  
  public boolean has_host() {
    // Note that hosts are special, absense of host means length 0.
    return _host.len > 0;
  }
  
  public boolean has_port() {
    return _port.len >= 0;
  }
  
  public boolean has_path() {
    // Note that http://www.google.com/" has a path, the path is "/". This can
    // return false only for invalid or nonstandard URLs.
    return _path.len >= 0;
  }
  
  public boolean has_query() {
    return _query.len >= 0;
  }
  
  public int getQueryOffset() {
    return _query.begin;
  }
  
  public int getQueryLen() { 
    return _query.len;
  }
  
  public boolean has_ref() {
    return _ref.len >= 0;
  }
  
  
  // Getters for various components of the URL. The returned string will be
  // empty if the component is empty or is not present.
  public String getScheme(){  // Not including the colon. See also SchemeIs.
    return getComponentString(_scheme);
  }
  public String getUserName(){
    return getComponentString(_userName);
  }
  public String getPassword(){
    return getComponentString(_password);
  }
  public String getHost(){
    return getComponentString(_host);
  }
  public GoogleURLComponent getHostComponent() { 
    return _host;
  }
  
  public String getPort(){  // Returns -1 if "default"
    return getComponentString(_port);
  }
  public String getPath(){  // Including first slash following host
    return getComponentString(_path);
  }
  public String getQuery(){  // Stuff following '?'
    return getComponentString(_query);
  }
  
  
  public String getRef(){  // Stuff following '#'
    return getComponentString(_ref);
  }

  public GoogleURLComponent getRefComponent(){  // Stuff following '#'
    return _ref;
  }
  
  public String getCanonicalURL() { 
    return _canonicalURL;
  } 
  
  public void dump() { 
    System.out.println("Scheme:" + getScheme());
    System.out.println("UserName:" + getUserName());
    System.out.println("Password:" + getPassword());
    System.out.println("Host:" + getHost());
    System.out.println("Port:" + getPort());
    System.out.println("Path:" + getPath());
    System.out.println("Query:" + getQuery());
    System.out.println("Ref:" + getRef());                        
  }
  
  private   String getComponentString(GoogleURLComponent comp) {
    if (_canonicalURL == null || comp.len <= 0)
      return emptyString;
    return _canonicalURL.substring(comp.begin,comp.begin + comp.len);
  }

    
  private boolean _isValid = false;
  private GoogleURLComponent _scheme    = new GoogleURLComponent();
  private GoogleURLComponent _userName  = new GoogleURLComponent();
  private GoogleURLComponent _password  = new GoogleURLComponent();
  private GoogleURLComponent _host      = new GoogleURLComponent();
  private GoogleURLComponent  _port     =  new GoogleURLComponent();
  private GoogleURLComponent _path      = new GoogleURLComponent();
  private GoogleURLComponent _query     = new GoogleURLComponent();
  private GoogleURLComponent _ref       = new GoogleURLComponent();
  private String  _canonicalURL = null;

  public static class GoogleURLComponent {

    public GoogleURLComponent() { 
      begin =0;
      len = -1;
    }

    // Normal constructor: takes an offset and a length.
    public GoogleURLComponent(int b, int l) { 
      begin = b;
      len = l;
    }

    public int end() {
      return begin + len;
    }

    // Returns true if this component is valid, meaning the length is given. Even
    // valid components may be empty to record the fact that they exist.
    public boolean is_valid() {
      return (len != -1);
    }

    // Returns true if the given component is specified on false, the component
    // is either empty or invalid.
    public boolean is_nonempty() {
      return (len > 0);
    }

    public void reset() {
      begin = 0;
      len = -1;
    }


    public int begin;  // Byte offset in the string of this component.
    public int len;    // Will be -1 if the component is unspecified.

  }  
  
  public static void main(String[] args) {
    GoogleURL urlObject = new GoogleURL("http://www.test.co.uk/somethingelse");
    System.out.println(urlObject.getCanonicalURL());
  }
}
