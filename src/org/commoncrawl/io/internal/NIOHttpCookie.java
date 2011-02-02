package org.commoncrawl.io.internal;

/**
 * abstract interface defining an http cookie 
 * 
 * @author rana
 *
 */

public interface NIOHttpCookie { 
  /**  
   * returns the domain name associated with this cookie
   * @return domain name string
   */
  public String Domain();
  /**  
   * returns the name associated with this cookie
   * @return name as string
   */
  public String Name();
  /**  
   * returns the value associated with this cookie
   * @return value as string
   */
  public String Value();
  /**  
   * returns the path associated with this cookie
   * @return path as string
   */
  public String Path();
  /**  
   * returns the creation date of this cookie 
   * @return date as a long value 
   */
  public long CreationDate();
  /**  
   * returns the last access date for this cookie 
   * @return date as a long value 
   */
  public long LastAccessDate();
  /**  
   * does this cookie have an expire time set  
   * @return returns a boolean (true) if expire time set
   */
  public boolean DoesExpire();
  /**  
   * is this a persistent cookie  
   * @return returns a boolean (true) if this cookie is persistent 
   */
  public boolean IsPersistent();
  /**  
   * returns the expiry date for this cookie 
   * @return expiry date as a long (UTC) 
   */
  public long ExpiryDate();
  /**  
   * is this cookie secure 
   * @return returns a boolean (true) if the cookie is secure 
   */
  public boolean IsSecure();
  
  /**  
   * is this an http only cookie 
   * @return returns a boolean (true) if the cookie is an http-only cookie 
   */
  public boolean IsHttpOnly();
}
