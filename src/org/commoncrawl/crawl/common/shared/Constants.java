package org.commoncrawl.crawl.common.shared;

public interface Constants {

  /** arc file header **/
  public static final String ARCFileHeader_ParseSegmentId = "x_commoncrawl_ParseSegmentId";
  public static final String ARCFileHeader_OriginalURL       = "x_commoncrawl_OriginalURL";
  public static final String ARCFileHeader_URLFP               = "x_commoncrawl_URLFP";
  public static final String ARCFileHeader_HostFP              = "x_commoncrawl_HostFP";
  public static final String ARCFileHeader_Signature           = "x_commoncrawl_Signature";
  public static final String ARCFileHeader_CrawlNumber     = "x_commoncrawl_CrawlNo";
  public static final String ARCFileHeader_CrawlerId          = "x_commoncrawl_CrawlerId";
  public static final String ARCFileHeader_FetchTimeStamp     = "x_commoncrawl_FetchTimestamp";
  public static final String ARCFileHeader_ContentTruncated     = "x-commoncrawl-ContentTruncated";
  public static final String ARCFileHeader_SOURCE_IS_GZIPED = "x_commoncrawl_SourceIsGZIP";

}
