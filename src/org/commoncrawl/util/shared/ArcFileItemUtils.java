package org.commoncrawl.util.shared;

import java.util.ArrayList;

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

  public static NIOHttpHeaders buildHeaderFromArcFileItemHeaders(
      ArrayList<ArcFileHeaderItem> items) {
    NIOHttpHeaders headers = new NIOHttpHeaders();

    for (ArcFileHeaderItem headerItem : items) {
      headers.add(headerItem.getItemKey(), headerItem.getItemValue());
    }

    return headers;
  }

}
