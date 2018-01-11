package org.commoncrawl.util.shared;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.commoncrawl.crawl.common.shared.Constants;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;

/**
 * Support routines related to ArcFileItem
 * 
 * @author rana
 * 
 */
public class ArcFileItemUtils {
  
  /** logging **/
  private static final Log LOG = LogFactory.getLog(ArcFileItemUtils.class);


  public static NIOHttpHeaders buildHeaderFromArcFileItemHeaders(
      ArrayList<ArcFileHeaderItem> items) {
    NIOHttpHeaders headers = new NIOHttpHeaders();

    for (ArcFileHeaderItem headerItem : items) {
      headers.add(headerItem.getItemKey(), headerItem.getItemValue());
    }

    return headers;
  }

  static     SimpleDateFormat       TIMESTAMP14   = new SimpleDateFormat(
      "yyyyMMddHHmmss");

  public static void bytesWritableToArcFileItem(Text key,BytesWritable rawArcPayload,ArcFileItem arcFileItem)throws IOException { 
    
    arcFileItem.clear();
    
    int indexOfTrailingCRLF = ByteArrayUtils.indexOf(rawArcPayload.getBytes(), 0, rawArcPayload.getLength(), "\r\n\r\n".getBytes());
    int headerLen = indexOfTrailingCRLF + 4;
    int contentLen = rawArcPayload.getLength() - headerLen;
    
    // parse headers 
    String headerStr = new TextBytes(rawArcPayload.getBytes(),0,headerLen,true).toString();
    NIOHttpHeaders headers = NIOHttpHeaders.parseHttpHeaders(headerStr);
    
    // extract appropriate header values to populate ArcFileItem 
    arcFileItem.getUriAsTextBytes().set(key, true);
    arcFileItem.setHostIP(headers.findValue(Constants.ARCFileHeader_HostIP));
    try {
      arcFileItem.setTimestamp(TIMESTAMP14.parse(headers.findValue(Constants.ARCFileHeader_ARC_Timestamp)).getTime());
    } catch (Exception e) {
      LOG.error("Invalid Timestamp Encountered in Item Metdata. URL:"
          + arcFileItem.getUri() + " Timestamp:" + headers.findValue(Constants.ARCFileHeader_ARC_Timestamp) );
    }
    arcFileItem.setMimeType(headers.findValue(Constants.ARCFileHeader_ARC_MimeType));
    arcFileItem.setRecordLength(rawArcPayload.getLength());
    
    //populate headers 
    for (int i=0;i<headers.getKeyCount();++i) { 
      String headerKey = headers.getKey(i);
      String headerValue = headers.getValue(i);
      
      ArcFileHeaderItem headerItem = new ArcFileHeaderItem();
      
      headerItem.setItemKey((headerKey != null) ? headerKey : "");
      headerItem.setItemValue((headerValue != null) ? headerValue : "");
      
      arcFileItem.getHeaderItems().add(headerItem);
    }
    // set up buffer pointer ... 
    arcFileItem.setContent(new FlexBuffer(rawArcPayload.getBytes(),headerLen,contentLen,true));
  }

}
