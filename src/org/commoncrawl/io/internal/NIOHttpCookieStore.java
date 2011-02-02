package org.commoncrawl.io.internal;

import java.net.URL;

/** 
 * abstract interface defining the functionality of a cookie store ... 
 * 
 * @author rana
 *
 */
public interface NIOHttpCookieStore { 
  
  /**
   * retrieve http cookies from the cookie store given a url
   * @param url the url idenitifying the domain/path for which cookies need to be retrieved
   * @return the http cookie string (if any) for the domain/path
   */
  public String GetCookies(URL url); 
  
  
  /**
   * set / update cookies in the cookie store given a url 
   * 
   * @param urlObject the url indentifying the domain/path context for the passed on cookie 
   * @param cookie the set cookie line, as received from the http protocol headers
   * @return (true) if setCookie operation succeeded
   */
  public boolean setCookie(URL urlObject,String cookie);

  
  
}
