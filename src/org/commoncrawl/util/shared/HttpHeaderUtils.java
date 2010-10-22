package org.commoncrawl.util.shared;

import java.util.Iterator;

import org.commoncrawl.io.shared.NIOHttpHeaders;

public class HttpHeaderUtils {

	public static final String kCharset= "charset=";
  
	public static class ContentTypeAndCharset { 
		
		public String _contentType = null;
		public String _charset    = null;
		
		void reset() { 
			_contentType = null;
			_charset = null;
		}
	}
	
	public static void parseContentType(NIOHttpHeaders headers,ContentTypeAndCharset metadataOut) {
    
    Iterator<String> j = headers.multiValueIterator("content-type");

    while (j.hasNext()) { 
      
      String contentType = j.next();
      
      if (contentType != null) { 
      	parseContentType(metadataOut,contentType);
      }
    }
  }  

  static final void  parseContentType(ContentTypeAndCharset metadataOut,String contentType) {
  	
  	// reset output data structure 
  	metadataOut.reset();
  	
    //  Trim leading and trailing whitespace from type.  We include '(' in
    //  the trailing trim set to catch media-type comments, which are not at all
    //  standard, but may occur in rare cases.
	  int type_val = skipPastLWS(contentType,0);
	  type_val = Math.min(type_val,contentType.length());
	  int type_end = skipToLWSAndExtra(contentType, type_val);
	  if (type_end == -1)
	    type_end = contentType.length();
	    
	  int charset_val = 0;
	  int charset_end = 0;
	
	  //  Iterate over parameters
	  boolean type_has_charset = false;
	  int param_start = contentType.indexOf(';', type_end);
	  if (param_start != -1) {
	    //    We have parameters.  Iterate over them.
	    int cur_param_start = param_start + 1;
	    do {
	      int cur_param_end = contentType.indexOf(';',cur_param_start);
	      if (cur_param_end == -1) 
	        cur_param_end = contentType.length();
	      int param_name_start = skipPastLWS(contentType,cur_param_start);
	      param_name_start = Math.min(param_name_start, cur_param_end);
	      int charset_end_offset = Math.min(param_name_start + kCharset.length(), cur_param_end);
	      if (contentType.substring(param_name_start,charset_end_offset).equalsIgnoreCase(kCharset)) {
	        charset_val = param_name_start + kCharset.length();
	        charset_end = cur_param_end;
	        type_has_charset = true;
	      }
	      cur_param_start = cur_param_end + 1;
	    } while (cur_param_start < contentType.length());
	  }
	
	  if (type_has_charset) {
	    try { 
	//    Trim leading and trailing whitespace from charset_val.  We include
	//    '(' in the trailing trim set to catch media-type comments, which are
	//    not at all standard, but may occur in rare cases.
	      charset_val = skipPastLWS(contentType,charset_val);
	      charset_val = Math.min(charset_val, charset_end);
	      if (charset_val == contentType.length()) { 
	        type_has_charset = false;
	      }
	      else { 
	        char first_char = contentType.charAt(charset_val);
	        if (first_char == '"' || first_char == '\'') {
	          ++charset_val;
	          charset_end = contentType.indexOf(first_char,charset_val);
	          if (charset_end == -1) 
	            charset_end = skipToLWSAndExtra(contentType,charset_val);
	        } else {
	          charset_end = Math.min(skipToLWSAndExtra(contentType,charset_val),charset_end);
	        }
	      }
	    }
	    catch (IndexOutOfBoundsException e) { 
	      type_has_charset = false;
	    }
	  }
	
	  //  if the server sent "*/*", it is meaningless, so do not store it.
	  //  also, if type_val is the same as mime_type, then just update the
	  //  charset.  however, if charset is empty and mime_type hasn't
	  //  changed, then don't wipe-out an existing charset.  We
	  //  also want to reject a mime-type if it does not include a slash.
	  //  some servers give junk after the charset parameter, which may
	  //  include a comma, so this check makes us a bit more tolerant.
	  if (contentType.length() != 0 && !contentType.equals("*/*") && contentType.indexOf('/') != -1) {
	    String originalContentType = metadataOut._contentType;
	    metadataOut._contentType = (contentType.substring(type_val,type_end).toLowerCase());
	    
	    if (type_has_charset) {
	    	metadataOut._charset = (contentType.substring(charset_val,charset_end).toLowerCase());
	    }
	    else {
	    	// ok content type changed but no charset found ... 
	      if (originalContentType != null && !originalContentType.equals(metadataOut._contentType)) { 
	      	// reset charset 
	      	metadataOut._charset = null;
	      }
	    }
	  }
  }

  public static int skipPastLWS(String sourceString,int startPos) { 
    int endPos = startPos;
    while (endPos < sourceString.length()) { 
      char c = sourceString.charAt(endPos);
      if (c != ' ' && c != '\t')
        break;
      endPos++;
    }
    return endPos;
  }

  public static int skipToLWSAndExtra(String sourceString,int startPos) { 
    int endPos = startPos;
    while (endPos < sourceString.length()) { 
      char c = sourceString.charAt(endPos);
      if (c == ' ' || c == '\t' || c == ';' || c == '(')
        break;
      endPos++;
    }
    return endPos;
  }
  
  
}
