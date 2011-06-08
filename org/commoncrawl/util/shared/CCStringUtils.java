package org.commoncrawl.util.shared;

/*
 *    Copyright 2010 - CommonCrawl Foundation
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * String related helpers - an extension of helpers in Hadoop StringUtils
 * 
 * @author rana
 * 
 */
public class CCStringUtils extends org.apache.hadoop.util.StringUtils {

  public static InetSocketAddress parseSocketAddress(String socketAddress) {

    if (socketAddress != null) {
      int colonIdx = socketAddress.indexOf(':');
      if (colonIdx != -1) {
        String hostName = socketAddress.substring(0, colonIdx);
        String port = socketAddress.substring(colonIdx + 1);
        return new InetSocketAddress(hostName, Integer.parseInt(port));
      }
    }

    return null;
  }

  private static SimpleDateFormat _formatter = new SimpleDateFormat(
                                                 "yyyy.MM.dd 'at' hh:mm:ss z");
  static {
    _formatter.setTimeZone(TimeZone.getTimeZone("PST"));
  }

  public static final String dateStringFromTimeValue(long timeValue) {

    if (timeValue != -1) {
      Date theDate = new Date(timeValue);
      return _formatter.format(theDate);
    }
    return "";
  }
}
