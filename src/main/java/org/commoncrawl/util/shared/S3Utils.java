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
 **/

package org.commoncrawl.util.shared;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.SAXException;

/**
 * 
 * Helper routines used to sign S3 requests 
 * 
 * @author rana
 * 
 */
public class S3Utils {

  static final String METADATA_PREFIX = "x-amz-meta-";
  static final String AMAZON_HEADER_PREFIX = "x-amz-";
  static final String ALTERNATIVE_DATE_HEADER = "x-amz-date";
  public static final String DEFAULT_HOST = "s3.amazonaws.com";

  public static final int SECURE_PORT = 443;
  public static final int INSECURE_PORT = 80;

  /**
   * HMAC/SHA1 Algorithm per RFC 2104.
   */
  private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

  static String makeCanonicalString(String method, String bucket, String key, Map pathArgs, Map headers) {
    return makeCanonicalString(method, bucket, key, pathArgs, headers, null);
  }

  /**
   * Calculate the canonical string. When expires is non-null, it will be used
   * instead of the Date header.
   */
  static String makeCanonicalString(String method, String bucketName, String key, Map pathArgs, Map headers,
      String expires) {
    StringBuffer buf = new StringBuffer();
    buf.append(method + "\n");

    // Add all interesting headers to a list, then sort them. "Interesting"
    // is defined as Content-MD5, Content-Type, Date, and x-amz-
    SortedMap interestingHeaders = new TreeMap();
    if (headers != null) {
      for (Iterator i = headers.keySet().iterator(); i.hasNext();) {
        String hashKey = (String) i.next();
        if (hashKey == null)
          continue;
        String lk = hashKey.toLowerCase();

        // Ignore any headers that are not particularly interesting.
        if (lk.equals("content-type") || lk.equals("content-md5") || lk.equals("date")
            || lk.startsWith(AMAZON_HEADER_PREFIX)) {
          List s = (List) headers.get(hashKey);
          interestingHeaders.put(lk, concatenateList(s));
        }
      }
    }

    if (interestingHeaders.containsKey(ALTERNATIVE_DATE_HEADER)) {
      interestingHeaders.put("date", "");
    }

    // if the expires is non-null, use that for the date field. this
    // trumps the x-amz-date behavior.
    if (expires != null) {
      interestingHeaders.put("date", expires);
    }

    // these headers require that we still put a new line in after them,
    // even if they don't exist.
    if (!interestingHeaders.containsKey("content-type")) {
      interestingHeaders.put("content-type", "");
    }
    if (!interestingHeaders.containsKey("content-md5")) {
      interestingHeaders.put("content-md5", "");
    }

    // Finally, add all the interesting headers (i.e.: all that startwith x-amz-
    // ;-))
    for (Iterator i = interestingHeaders.keySet().iterator(); i.hasNext();) {
      String headerKey = (String) i.next();
      if (headerKey.startsWith(AMAZON_HEADER_PREFIX)) {
        buf.append(headerKey).append(':').append(interestingHeaders.get(headerKey));
      } else {
        buf.append(interestingHeaders.get(headerKey));
      }
      buf.append("\n");
    }

    // build the path using the bucket and key
    if (bucketName != null && !bucketName.equals("")) {
      buf.append("/" + bucketName);
    }

    // append the key (it might be an empty string)
    // append a slash regardless
    buf.append("/");
    if (key != null) {
      buf.append(key);
    }

    // if there is an acl, logging or torrent parameter
    // add them to the string
    if (pathArgs != null) {
      if (pathArgs.containsKey("acl")) {
        buf.append("?acl");
      } else if (pathArgs.containsKey("torrent")) {
        buf.append("?torrent");
      } else if (pathArgs.containsKey("logging")) {
        buf.append("?logging");
      } else if (pathArgs.containsKey("location")) {
        buf.append("?location");
      }
    }

    return buf.toString();

  }

  /**
   * Calculate the HMAC/SHA1 on a string.
   * 
   * @param data
   *          Data to sign
   * @param passcode
   *          Passcode to sign it with
   * @return Signature
   * @throws NoSuchAlgorithmException
   *           If the algorithm does not exist. Unlikely
   * @throws InvalidKeyException
   *           If the key is invalid.
   */
  static String encode(String awsSecretAccessKey, String canonicalString, boolean urlencode) {
    // The following HMAC/SHA1 code for the signature is taken from the
    // AWS Platform's implementation of RFC2104
    // (amazon.webservices.common.Signature)
    //
    // Acquire an HMAC/SHA1 from the raw key bytes.
    SecretKeySpec signingKey = new SecretKeySpec(awsSecretAccessKey.getBytes(), HMAC_SHA1_ALGORITHM);

    // Acquire the MAC instance and initialize with the signing key.
    Mac mac = null;
    try {
      mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      // should not happen
      throw new RuntimeException("Could not find sha1 algorithm", e);
    }
    try {
      mac.init(signingKey);
    } catch (InvalidKeyException e) {
      // also should not happen
      throw new RuntimeException("Could not initialize the MAC algorithm", e);
    }

    // Compute the HMAC on the digest, and set it.
    String b64 = Base64.encodeBytes(mac.doFinal(canonicalString.getBytes()));

    if (urlencode) {
      return urlencode(b64);
    } else {
      return b64;
    }
  }

  static Map paramsForListOptions(String prefix, String marker, Integer maxKeys) {
    return paramsForListOptions(prefix, marker, maxKeys, null);
  }

  static Map paramsForListOptions(String prefix, String marker, Integer maxKeys, String delimiter) {

    Map argParams = new HashMap();
    // these three params must be url encoded
    if (prefix != null)
      argParams.put("prefix", urlencode(prefix));
    if (marker != null)
      argParams.put("marker", urlencode(marker));
    if (delimiter != null)
      argParams.put("delimiter", urlencode(delimiter));

    if (maxKeys != null)
      argParams.put("max-keys", Integer.toString(maxKeys.intValue()));

    return argParams;

  }

  /**
   * Converts the Path Arguments from a map to String which can be used in url
   * construction
   * 
   * @param pathArgs
   *          a map of arguments
   * @return a string representation of pathArgs
   */
  public static String convertPathArgsHashToString(Map pathArgs) {
    StringBuffer pathArgsString = new StringBuffer();
    String argumentValue;
    boolean firstRun = true;
    if (pathArgs != null) {
      for (Iterator argumentIterator = pathArgs.keySet().iterator(); argumentIterator.hasNext();) {
        String argument = (String) argumentIterator.next();
        if (firstRun) {
          firstRun = false;
          pathArgsString.append("?");
        } else {
          pathArgsString.append("&");
        }

        argumentValue = (String) pathArgs.get(argument);
        pathArgsString.append(argument);
        if (argumentValue != null) {
          pathArgsString.append("=");
          pathArgsString.append(argumentValue);
        }
      }
    }

    return pathArgsString.toString();
  }

  static String urlencode(String unencoded) {
    try {
      return URLEncoder.encode(unencoded, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      // should never happen
      throw new RuntimeException("Could not url encode to UTF-8", e);
    }
  }

  static XMLReader createXMLReader() {
    try {
      return XMLReaderFactory.createXMLReader();
    } catch (SAXException e) {
      // oops, lets try doing this (needed in 1.4)
      System.setProperty("org.xml.sax.driver", "org.apache.crimson.parser.XMLReaderImpl");
    }
    try {
      // try once more
      return XMLReaderFactory.createXMLReader();
    } catch (SAXException e) {
      throw new RuntimeException("Couldn't initialize a sax driver for the XMLReader");
    }
  }

  /**
   * Concatenates a bunch of header values, seperating them with a comma.
   * 
   * @param values
   *          List of header values.
   * @return String of all headers, with commas.
   */
  private static String concatenateList(List values) {
    StringBuffer buf = new StringBuffer();
    for (int i = 0, size = values.size(); i < size; ++i) {
      buf.append(((String) values.get(i)).replaceAll("\n", "").trim());
      if (i != (size - 1)) {
        buf.append(",");
      }
    }
    return buf.toString();
  }

  static boolean isValidSubdomainBucketName(String bucketName) {
    final int MIN_BUCKET_LENGTH = 3;
    final int MAX_BUCKET_LENGTH = 63;
    // don't allow names that look like 127.0.0.1
    final String IPv4_REGEX = "^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$";
    // dns sub-name restrictions
    final String BUCKET_NAME_REGEX = "^[a-z0-9]([a-z0-9\\-]*[a-z0-9])?(\\.[a-z0-9]([a-z0-9\\-]*[a-z0-9])?)*$";

    // If there wasn't a location-constraint, then the current actual
    // restriction is just that no 'part' of the name (i.e. sequence
    // of characters between any 2 '.'s has to be 63) but the recommendation
    // is to keep the entire bucket name under 63.
    return null != bucketName && bucketName.length() >= MIN_BUCKET_LENGTH && bucketName.length() <= MAX_BUCKET_LENGTH
        && !bucketName.matches(IPv4_REGEX) && bucketName.matches(BUCKET_NAME_REGEX);
  }

  /**
   * Amazon S3 utilities (from their deprecated API) 
   * 
   * @author rana
   *
   */
  public abstract static class CallingFormat {

    protected static CallingFormat pathCallingFormat = new PathCallingFormat();
    protected static CallingFormat subdomainCallingFormat = new SubdomainCallingFormat();
    protected static CallingFormat vanityCallingFormat = new VanityCallingFormat();

    public abstract boolean supportsLocatedBuckets();

    public abstract String getEndpoint(String server, int port, String bucket);

    public abstract String getPathBase(String bucket, String key);

    public abstract URL getURL(boolean isSecure, String server, int port, String bucket, String key, Map pathArgs)
        throws MalformedURLException;

    public static CallingFormat getPathCallingFormat() {
      return pathCallingFormat;
    }

    public static CallingFormat getSubdomainCallingFormat() {
      return subdomainCallingFormat;
    }

    public static CallingFormat getVanityCallingFormat() {
      return vanityCallingFormat;
    }

    static private class PathCallingFormat extends CallingFormat {
      public boolean supportsLocatedBuckets() {
        return false;
      }

      public String getPathBase(String bucket, String key) {
        return isBucketSpecified(bucket) ? "/" + bucket + "/" + key : "/";
      }

      public String getEndpoint(String server, int port, String bucket) {
        return server + ":" + port;
      }

      public URL getURL(boolean isSecure, String server, int port, String bucket, String key, Map pathArgs)
          throws MalformedURLException {
        String pathBase = isBucketSpecified(bucket) ? "/" + bucket + "/" + key : "/";
        String pathArguments = convertPathArgsHashToString(pathArgs);
        return new URL(isSecure ? "https" : "http", server, port, pathBase + pathArguments);
      }

      private boolean isBucketSpecified(String bucket) {
        if (bucket == null)
          return false;
        if (bucket.length() == 0)
          return false;
        return true;
      }
    }

    static private class SubdomainCallingFormat extends CallingFormat {
      public boolean supportsLocatedBuckets() {
        return true;
      }

      public String getServer(String server, String bucket) {
        return bucket + "." + server;
      }

      public String getEndpoint(String server, int port, String bucket) {
        return getServer(server, bucket) + ":" + port;
      }

      public String getPathBase(String bucket, String key) {
        return "/" + key;
      }

      public URL getURL(boolean isSecure, String server, int port, String bucket, String key, Map pathArgs)
          throws MalformedURLException {
        if (bucket == null || bucket.length() == 0) {
          // The bucket is null, this is listAllBuckets request
          String pathArguments = convertPathArgsHashToString(pathArgs);
          return new URL(isSecure ? "https" : "http", server, port, "/" + pathArguments);
        } else {
          String serverToUse = getServer(server, bucket);
          String pathBase = getPathBase(bucket, key);
          String pathArguments = convertPathArgsHashToString(pathArgs);
          return new URL(isSecure ? "https" : "http", serverToUse, port, pathBase + pathArguments);
        }
      }
    }

    static private class VanityCallingFormat extends SubdomainCallingFormat {
      public String getServer(String server, String bucket) {
        return bucket;
      }
    }
  }


  /**
   * Validate bucket-name
   */
  static boolean validateBucketName(String bucketName, CallingFormat callingFormat, boolean located) {
    if (callingFormat == CallingFormat.getPathCallingFormat()) {
      final int MIN_BUCKET_LENGTH = 3;
      final int MAX_BUCKET_LENGTH = 255;
      final String BUCKET_NAME_REGEX = "^[0-9A-Za-z\\.\\-_]*$";

      return null != bucketName && bucketName.length() >= MIN_BUCKET_LENGTH
          && bucketName.length() <= MAX_BUCKET_LENGTH && bucketName.matches(BUCKET_NAME_REGEX);
    } else {
      return isValidSubdomainBucketName(bucketName);
    }
  }

  static CallingFormat getCallingFormatForBucket(CallingFormat desiredFormat, String bucketName) {
    CallingFormat callingFormat = desiredFormat;
    if (callingFormat == CallingFormat.getSubdomainCallingFormat() && !isValidSubdomainBucketName(bucketName)) {
      callingFormat = CallingFormat.getPathCallingFormat();
    }
    return callingFormat;
  }

}
