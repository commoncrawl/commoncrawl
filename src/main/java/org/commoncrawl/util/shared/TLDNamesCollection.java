package org.commoncrawl.util.shared;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.util.shared.CCStringUtils;

import com.google.common.collect.ImmutableMultimap;

public class TLDNamesCollection {

  private static final Log LOG = LogFactory.getLog(TLDNamesCollection.class);
	
	public static Collection<String> getSecondaryNames(String tldName) { 
		initialize();
		return tldToSecondaryNameMap.get(tldName);
	}
	
	private static ImmutableMultimap<String, String> tldToSecondaryNameMap = null;
	private static void initialize() { 
		synchronized (TLDNamesCollection.class) {
	    if (tldToSecondaryNameMap == null) { 

	    	try {
	    		
	    		ImmutableMultimap.Builder<String, String> builder = new ImmutableMultimap.Builder<String,String>();
	    		
	    			    		
	  		  InputStream inputStream = ClassLoader.getSystemClassLoader().getResourceAsStream("effective_tld_list.txt");
	  		  
	  		  try { 
	  		  
	  		  	BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));
	  	  	
	  		  	String line = null;
	  		  	
	  		  			  	
	  		  	while ( (line = reader.readLine()) != null) { 
	  		  		if (!line.startsWith("//")) { 
	  		  			if (line.length() != 0) { 
	  			  			int indexOfDot = line.lastIndexOf(".");
	  			  			if (indexOfDot == -1) { 
	  			  				builder.put(line.trim(),"");
	  			  			}
	  			  			else { 
	  			  				String leftSide = line.substring(0,indexOfDot).trim();
	  			  				String rightSide = line.substring(indexOfDot + 1).trim();
	  			  				builder.put(rightSide,leftSide);
	  			  			}
	  		  			}
	  		  		}
	  		  	}
	  		  	
	  		  	tldToSecondaryNameMap = builder.build();
	  		  }
	  		  finally { 
	  		  	inputStream.close();
	  		  }
	  	  }
	  	  catch (IOException e) { 
	  	  	LOG.error(CCStringUtils.stringifyException(e));
	  	  	throw new RuntimeException(e);
	  	  }
			}
		}
	}
}
