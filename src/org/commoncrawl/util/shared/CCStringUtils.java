package org.commoncrawl.util.shared;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.util.StringUtils;

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
				String hostName = socketAddress.substring(0,colonIdx);
				String port           = socketAddress.substring(colonIdx + 1);
				return new InetSocketAddress(hostName,Integer.parseInt(port));
			}
		}
		
		return null;
	}
	
	
  private static SimpleDateFormat _formatter = new SimpleDateFormat("yyyy.MM.dd 'at' hh:mm:ss z");
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
