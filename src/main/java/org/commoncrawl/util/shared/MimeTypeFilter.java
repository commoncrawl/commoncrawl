package org.commoncrawl.util.shared;

import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.util.shared.HttpHeaderUtils.ContentTypeAndCharset;

public class MimeTypeFilter {

  private final static Pattern       TRUNCATION_REGEX       = Pattern
                                                                .compile("^([^a-zA-Z0-9]*)([a-zA-Z0-9-/\\.+]*).*");
  private final static Pattern       APPLICATION_TYPE_REGEX = Pattern
                                                                .compile("^application/(.*)");
  
  private final static Pattern       TYPE_SUBTYPE_EXTRACTION_REGEX = Pattern
                                                          .compile("(.+)/(.+)");
  
  /*
  private final static Pattern       LOOSE_TEXT_SUBTYPES_MATCHER = Pattern
                                                          .compile("[\\-\\.]*([^\\+]*\\+)*(txt|text|plain|html|atom|xml|xhtml|css|pdf|postscript|dtd|rss|vcard|rtf|csv|javascript|json)(\\+.*)*");
  */
  private final static Pattern       SUBTYPE_LEFT_AND_RIGHT_PART_MATCHER = Pattern
                                                                    .compile("[\\-\\.\\+]*(x\\-|vnd\\.)*([^\\+]*)\\+*([a-z]*)(\\+.*)*");
  
  private final static Pattern       TEXT_SUBTYPES_MATCHER = Pattern
  .compile("(txt|text|plain|html|atom|xml|xhtml|css|pdf|postscript|dtd|rss|vcard|rtf|csv|javascript|json|perl|ruby|java|text-javascript|asp|php|doc|py|c|cc|c++|cxx|m|h)");

  private final static Pattern       HTML_SUBTYPES_MATCHER = Pattern
  .compile("(xhtml|html)");

  /*  
  private final static Pattern       HTML_PARSER_COMPATIBLE_TEXT_SUBTYPES_MATCHER = Pattern
  .compile("[\\-\\.]*(([^\\+]*\\+)*(xhtml|html)(\\+xml)*");
  */
  
  public static final String         NO_TYPE_MIMETYPE       = "no-type";
  private static PrefixStringMatcher knownBinaryTypesMatcher;
  private static SuffixStringMatcher invalidExtensionMatcher;
  private static SuffixStringMatcher textValidAppTypesMatcher;
  private static SuffixStringMatcher otherValidAppTypesMatcher;
  private static PrefixStringMatcher otherExcludedPrimaryMimeTypeMatcher;
  private static PrefixStringMatcher alwaysValidPrimaryTextTypesMatcher;
  private static String knownBinaryTypes[];
  private static String validPrimaryTextTypes[];
  private static String invalidExtensions[];
  private static String textValidApplicationTypes[];
  private static String otherValidApplicationTypes[];
  

  static {

    knownBinaryTypes = new String[]{ "binary",
      "octet","file",
      "image", "audio", "video", "true-type", "x-epoc" };

    knownBinaryTypesMatcher = new PrefixStringMatcher(knownBinaryTypes);

    String otherExcludedPrimaryMimeTypes[] = { "chemical", "plugin" };

    otherExcludedPrimaryMimeTypeMatcher = new PrefixStringMatcher(otherExcludedPrimaryMimeTypes);
    
    validPrimaryTextTypes = new String[]{ "text", "plain", "message","multipart" };
    
    alwaysValidPrimaryTextTypesMatcher = new PrefixStringMatcher(validPrimaryTextTypes);

    invalidExtensions = new String[] { "gif", "jpg", "jpeg", "bmp", "png", "tif",
      "tiff",
      "ico",
      // "eps",
      "ps", "wmf", "fpx", "cur",
      "ani",
      "img",
      "lwf",
      "pcd",
      "psp",
      // "psd",
      "tga",
      "xbm",
      "xpm",
      "arj",
      // "arc",
      // "7z",
      "cab",
      "lzw",
      "lha",
      "lzh",
      // "zip",
      // "gz",
      // "tar",
      // "tgz",
      "sit", "rpm", "deb", "pkg", "mid", "midi", "rmi", "mpeg", "mpg", "mpe",
      "mp3", "mp2", "aac",
      "mov",
      // "fla",
      // "flv",
      "ra", "ram", "rm", "rmv", "wma", "wmv", "wav", "wave", "ogg", "avi",
      "au", "snd", "exe", "com", "lnk", "t3x", "iso", "bin", "dwf", "asf" };

    invalidExtensionMatcher = new SuffixStringMatcher(invalidExtensions);

    textValidApplicationTypes = 
      new String[] { 
        
        "wap.xhtml+xml",
        "acrobat", 
        "acroread",
        "dot", 
        "texinfo", 
        "tex",
        "text-java", 
        "oasis.opendocument.text",
        "sun.xml.writer",
        "groove-vcard",
        "ms-works",
        "pdf",
        "word-doc",
        "httpd-php",
        "httpd-php-source",
        "httpd.php",
        "mscardfile",
        "server-parsed-html",
        "xml-dtd",
    };
    
    otherValidApplicationTypes = 
    new String[] {
      "tar", 
      "latex", 
      "wordperfect5.1",
      "wordperfect", 
      "winhlp",
      "google-earth.kml", 
      "google-earth.kml+xml",
      "google-earth.kml+xml kml", 
      "google-earth.kmz",
      "google-earth.kmz kmz",
      "openxmlformat", 
      "powerpoint",
      "ms-powerpoint",
      "mspowerpoint",
      "keyhole", 
      "kml", 
      "ms-excel", 
      "msaccess",
      "msexcel", 
      "excel",  
      "oasis.opendocument.spreadsheet",
      "oasis.opendocument.presentation",
      "oasis.opendocument.spreadsheet",
      "openxmlformats",
      "sun.xml.calc",
      "cardfile",
      "mspublisher",
      "shockwave-flash",

      "openxmlformat",
      "lotus",
      "ms-word",
      "msword",
      "ms-htmlhelp",
      "ms-project",
      "msword-doc",
      "xls"
    };

    textValidAppTypesMatcher = new SuffixStringMatcher(textValidApplicationTypes);
    otherValidAppTypesMatcher = new SuffixStringMatcher(otherValidApplicationTypes);
    
    
    String validMessageTypes[] = { 
        "news",
        "rfc822"
    };

    
    String validMultipartTypes[] = { 
        "form-data",
        "x-mixed-replace"
    };
    
            
  }

  private static String truncateMimeType(String contentType) {
    if (contentType == null) {
      contentType = NO_TYPE_MIMETYPE;
    } else {
      Matcher matcher = TRUNCATION_REGEX.matcher(contentType);
      if (matcher.matches()) {
        contentType = matcher.group(2);
      } else {
        contentType = NO_TYPE_MIMETYPE;
      }
    }
    return contentType;
  }

  public static String normalizeMimeType(String mimeTypeIn) {
    return truncateMimeType(mimeTypeIn).toLowerCase();
  }

  public enum MimeTypeDisposition {
    ACCEPT_TEXT,
    ACCEPT_HTML,
    ACCEPT_OTHER,
    REJECT
  }
  
  
  public static MimeTypeDisposition checkMimeTypeDisposition(String mimeType) {

    if (mimeType == null)
      return MimeTypeDisposition.ACCEPT_OTHER;
    // first normalize the string ...
    String truncatedMimeType = truncateMimeType(mimeType).toLowerCase();

    if (truncatedMimeType == NO_TYPE_MIMETYPE) {  
      return MimeTypeDisposition.ACCEPT_OTHER;
    }
    
    else {
      // do a type sub-type match first ... 
      Matcher type_subtype_matcher = TYPE_SUBTYPE_EXTRACTION_REGEX.matcher(truncatedMimeType);
      
      String primaryType = null;
      String secondaryType = null;
      
      // if string is a type/subtype record 
      if (type_subtype_matcher.matches()) { 
        // primary type is first match element
        primaryType = type_subtype_matcher.group(1);
        secondaryType = type_subtype_matcher.group(2);
      }
      // else if string is strictly a type record 
      else { 
        primaryType = truncatedMimeType;
      }
      // now if primary type is null or empty ... 
      if (primaryType == null || primaryType.length() == 0) { 
        // accept by default ...  
        return MimeTypeDisposition.ACCEPT_OTHER; 
      }
      else {
        // first check for outright exclusion via primary type 
        if (knownBinaryTypesMatcher.matches(primaryType) || 
            otherExcludedPrimaryMimeTypeMatcher.matches(primaryType)) {
          // reject known binary types ... 
          return MimeTypeDisposition.REJECT; 
        }
        // check for inclusion via primary type 
        else if (alwaysValidPrimaryTextTypesMatcher.matches(primaryType)) { 
          if (secondaryType != null) {
            Matcher m = SUBTYPE_LEFT_AND_RIGHT_PART_MATCHER.matcher(secondaryType);
            if (m.matches()) { 
              for (int i=2;i<=3;++i) { 
                if (m.group(i) != null 
                    &&
                    (m.group(i).equals("html")
                    || m.group(i).equals("xhtml"))) { 

                  return MimeTypeDisposition.ACCEPT_HTML;
                }
              }
            }
          }
          // accept no matter what secondary type is ...  
          return MimeTypeDisposition.ACCEPT_TEXT; 
        }
        
        // check for invalid extension types ...
        if ((secondaryType != null && invalidExtensionMatcher.exactMatch(secondaryType))
            || 
            invalidExtensionMatcher.exactMatch(primaryType)) {
          // ok reject this guy ... 
          return MimeTypeDisposition.REJECT;
        }
        
        Matcher typeSubTypeMatcher = (secondaryType != null) ? 
            SUBTYPE_LEFT_AND_RIGHT_PART_MATCHER.matcher(secondaryType) : 
              SUBTYPE_LEFT_AND_RIGHT_PART_MATCHER.matcher(primaryType);
            
        // ok now see if a secondary type is valid text type .. 
        if (typeSubTypeMatcher.matches()) {
          for (int i=2;i<=3;++i) { 
            if (typeSubTypeMatcher.group(i) != null 
                &&
                (typeSubTypeMatcher.group(i).equals("html")
                || typeSubTypeMatcher.group(i).equals("xhtml"))) { 

              return MimeTypeDisposition.ACCEPT_HTML;
            }
          }
          // next try text types ... 
          for (int i=2;i<=3;++i) { 
            if (typeSubTypeMatcher.group(i) != null 
                && TEXT_SUBTYPES_MATCHER.matcher(typeSubTypeMatcher.group(i)).matches()) { 
              return MimeTypeDisposition.ACCEPT_TEXT;
            }
          }
        }
        
        // ok last but not least ...  
        // if this is an application type ...  
        if (secondaryType != null) { 
          if (secondaryType.startsWith("vnd.")) {
            secondaryType = secondaryType.substring("vnd.".length());
          }
          else if (secondaryType.startsWith("x-")) { 
            secondaryType = secondaryType.substring("x-".length());
          }
          // if secondary type matches accept application type
          if (textValidAppTypesMatcher.exactMatch(secondaryType)) { 
            return MimeTypeDisposition.ACCEPT_TEXT;
          }
          if (otherValidAppTypesMatcher.exactMatch(secondaryType)) { 
            return MimeTypeDisposition.ACCEPT_OTHER;  
          }
        }
      }
      // reject by default ... 
      return MimeTypeDisposition.REJECT;
    }
  }

  /**
   * a looser interpretation of what is a text type
   * 
   * @param contentTypeStr
   * @return
   */
  public static boolean isTextType(String contentTypeStr) {
    MimeTypeDisposition disposition = checkMimeTypeDisposition(contentTypeStr);
    if (disposition == MimeTypeDisposition.ACCEPT_TEXT || 
        disposition == MimeTypeDisposition.ACCEPT_HTML) { 
      return true;
    }
    return false;
  }

  /**
   * more restrictive interpretation of what is a valid html type
   */
  public static boolean isValidHTMLType(String contentTypeStr) {
    return checkMimeTypeDisposition(contentTypeStr) 
                                          == MimeTypeDisposition.ACCEPT_HTML;
  }
  
  private static class TestRecord {
    
    TestRecord(String testString,MimeTypeDisposition disposition,MimeTypeDisposition overrideDisposition) { 
      _testString = testString;
      _expectedDisposition = (overrideDisposition == null) ? disposition : overrideDisposition;
    }
    
    String              _testString;
    MimeTypeDisposition _expectedDisposition;
  }
  
  private static void addBinaryTestRecords(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    for (String binaryType : knownBinaryTypes) { 
      testRecordList.add(new TestRecord(optionalPrefix + binaryType+"/foo" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    }
    for (String binaryType : knownBinaryTypes) { 
      testRecordList.add(new TestRecord(optionalPrefix + binaryType + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    }
  }
  private static void addValidTextTypeRecords(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    for (String textType : validPrimaryTextTypes) { 
      testRecordList.add(new TestRecord(optionalPrefix + textType+"/foo" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    }
    for (String textType : validPrimaryTextTypes) { 
      testRecordList.add(new TestRecord(optionalPrefix + textType + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    }
  }
  private static void addValidHTMLTypeRecords(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    testRecordList.add(new TestRecord(optionalPrefix + "text/html" +optionalSuffix,MimeTypeDisposition.ACCEPT_HTML,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/xhtml+xml" +optionalSuffix,MimeTypeDisposition.ACCEPT_HTML,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/xhtml" +optionalSuffix,MimeTypeDisposition.ACCEPT_HTML,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "html" +optionalSuffix,MimeTypeDisposition.ACCEPT_HTML,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "xhtml+xml" +optionalSuffix,MimeTypeDisposition.ACCEPT_HTML,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "xhtml" +optionalSuffix,MimeTypeDisposition.ACCEPT_HTML,overrideDisposition));
  }
  
  private static void addOtherValidTextTypeRecords(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) {
    
    testRecordList.add(new TestRecord(optionalPrefix + "txt" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "plain" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "atom" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "xml" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "css" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "pdf" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "postscript" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "dtd" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "rss" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "vcard" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "rtf" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "csv" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "javascript" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "json" +optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));   
  }

  
  private static void addTestApplicationStrings(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    testRecordList.add(new TestRecord(optionalPrefix + "application" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "asf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/pdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/acad" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/acrobat" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/applefile" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/asx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/atom" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/atom+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/atomserv+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/binary" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/cadstd" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/cap" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/cgi" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/doc" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/download" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/exe" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/filemaker" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/finale" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/fml" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/force-download" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/garmin-data" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/gpx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/java-archive" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/java-vm" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/javascript" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/json" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/keyhole" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/kml" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/kml+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/labview.app" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/lotus" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/mac-binhex40" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/maple" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/marc" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/mathematica" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/mathematica-package" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/metalink+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/mplayer2" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/mp_arc-mixed" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/ms-powerpoint" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/ms-word" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/msaccess" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/msi" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/mspowerpoint" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/msstreets" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/msword" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/nwc" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/octect-stream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/octet-stream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/octetstream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/octet_stream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/opensearchdescription+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/pdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/pdslabel" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/perl" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/pff" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/pgp-keys" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/pgp-signature" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/pls+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/plt-scheme-package" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/postscript" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/powerpoint" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/pps" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/prc" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/prs.plucker" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/rar" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/rdf+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/riscos" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/rsd" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/rsd+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/rss+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/rtf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/save" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/smil" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/smil+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/tabledit" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/TEXT" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/text-javascript" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/text-java" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/ti" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/torrent" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/unknown" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/unknown15" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/unkown" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vcard" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.adobe.air-application-installer-package+zip" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.adobe.apollo-application-installer-package+zip" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.dpgraph" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.google-earth.kml" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.google-earth.kml+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.google-earth.kmz" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.groove-vcard" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.mozilla.xul+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.ms-asf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.ms-excel" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.ms-htmlhelp" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.ms-powerpoint" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.ms-project" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.ms-word" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.ms-works" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.musician" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.oasis.opendocument.graphics" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.oasis.opendocument.presentation" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.oasis.opendocument.spreadsheet" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.oasis.opendocument.text" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.openxmlformats" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.palm" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.pdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.sun.xml.calc" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.sun.xml.draw." + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.sun.xml.impress" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.sun.xml.writer" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.symbian.install" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.wap.xhtml+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/vnd.wordperfect" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/winhlp" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/word-doc" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/wordperfect" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/wordperfect5.1" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-7z-compressed" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-acroread" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-amstex" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-apple-diskimage" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-arachne-package" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-asap" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-asp" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-autocad" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-bio" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-bittorrent" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-blorb" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-bzip" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-bzip2" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-cdf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-cdlink" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-chat" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-chess-pgn" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-compress" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-csh" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-director" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-doc" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-dot" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-download" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-dreamcast-vms-info" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-dvi" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-dzip" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-endnote-refer" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-executable" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-flac" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-font-ttf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-forcedownload" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-glulx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-go-sgf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-graphing-calculator" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-gzip" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-httpd-php" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-httpd-php-source" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-httpd-php3" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-httpd-php4" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-httpd-php5" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-httpd.php" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-hwp" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-imagemap" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-incredimail" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-install" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-install-instructions" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-intkey" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-ip" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-ipix" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-iso9660-image" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-jar" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-java" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-java-archive" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-java-jnlp-file" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-javascript" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-koan" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-kpresenter" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-latex" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-life" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-maker" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-makeself" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-maple" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-midi" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-mpeg4" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-mplayer2" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-ms-reader" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-ms-wmd" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-ms-wmz" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-mscardfile" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-msdos-program" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-msdownload" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-msi" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-mspublisher" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-msword" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-msword-doc" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-netcdf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-nwc" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-octet-stream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-octetstream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-opera-configuration-language" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-opera-configuration-menu" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-pdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-perl" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-php" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-pilot" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-qcshow" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-quicktime-media-link" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-quicktimeplayer" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-rar" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-rar-compressed" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-redhat-package-manager" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "Application/x-research-info-systems" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-server-parsed-html" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-sh" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-shar" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-shellscript" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-shockwave-flash" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-shockwave-flash2-preview" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-smaf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-stuffit" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-stuffitx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-sysquake" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-tar" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-tcl" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-tex" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-texinfo" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-trash" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-troff" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-troff-man" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-troff-ms" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-unknown" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-unknown-content-type-pdbFile" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-vmarc" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-wais-source" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-windows-gadget" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-wp" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-x509-ca-cert" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-x509-cert" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-xfig" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-xpinstall" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-zip" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-zip-compressed" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x-zmachine" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/x.atom+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/xhtml+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_HTML,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/xls" + optionalSuffix,MimeTypeDisposition.ACCEPT_OTHER,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/xml-dtd" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/xslt+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/xspf+xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/xyz123" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/zip" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "application/{$sAppType}" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));    
  }
  
  private static void addAudioTestRecords(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    testRecordList.add(new TestRecord(optionalPrefix + "audio-x/mpegurl" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/basic" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/m3u" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/m4a" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/m4b" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/midi" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/mp3" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/mp4" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/mp4a-latm" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/mpeg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/mpeg3" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/mpegurl" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/mpg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/playlist" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/quicktime" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/reason" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/scpls" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/unknown" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-aiff" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-m3u" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-m4a" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-m4b" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-mp3" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-mpeg," + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-mpegurl" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-mpegurl.m3u" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-mpequrl" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-ms-asf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-ms-asx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-ms-wax" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-ms-wma" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-pd" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-pn-realaudio" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-ptb" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-scpls" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-twinvq" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "audio/x-wav" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));    
  }
  
  private static void addImageTestRecords(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    testRecordList.add(new TestRecord(optionalPrefix + "image" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/*" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/autocad" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/bmp" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/gif" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/ico" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/jp2" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/jpeg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/jpeg,image/pjpeg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/jpg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/pak" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/pcx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/pdf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/pjpeg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/pk3" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/png" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/spr" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/svg+xml" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/tiff" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/vnd" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/vnd.djvu" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/vnd.dwg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/vnd.dxf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/vnd.ms-modi" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/vnd.nok-oplogo-color" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/vnd.wap.wbmp" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-coreldraw" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-dcraw" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-djvu" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-icon" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-ms-bmp" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-pcx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-pict" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-png" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-portable-pixmap" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-rgb" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-urt" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x-xbitmap" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "image/x.djvu" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "images/jpeg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));    
  }
  
  private static void addTestTextRecords(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    testRecordList.add(new TestRecord(optionalPrefix + "text/abc.vnd" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/bib" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/calendar" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/comma-separated-values" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/css" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/csv" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/directory" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/htm" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/html" + optionalSuffix,MimeTypeDisposition.ACCEPT_HTML,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/html/srt" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/javascript" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/js" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/json" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/langfile" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/pdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/plain" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/rdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/rdf+n3" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/richtext" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/rss" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/rtf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/sgml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/tab-separated-values" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/texmacs" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/text" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/troff" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/vbscript" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/vcard" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/vnd.abc" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/vnd.sun.j2me.app-descriptor" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/vnd.wap.wml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-bibtex" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-c" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-c++hdr" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-c++src" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-card" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-chdr" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-comma-separated-values" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-config" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-csrc" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-diff" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-dsrc" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-emacs-lisp" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-fortran" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-hamlet-action" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-hdml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-ical" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-java" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-java-source" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-log" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-opml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-pascal" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-patch" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-perl" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-python" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-registry" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-scheme" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-server-parsed-html" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-sh" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-sql" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-suse-ymp" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-tex" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-vCalendar" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-vCard" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/x-versit" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text/xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
  }
  
  private static void addTestVideoRecords(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    testRecordList.add(new TestRecord(optionalPrefix + "video/3gpp" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/asx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/flc" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/m4v" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/mp4" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/mp4v-es" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/mpeg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/quicktime" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/unknown" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-fli" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-m4v" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-mp4" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-ms-af" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-ms-asf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-ms-asf-plugin" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-ms-asx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-ms-wax" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-ms-wmv" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-ms-wmx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-ms-wvx" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-msvideo" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-pv-mp4" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "video/x-unknown" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));    
  }
  
  private static void addMiscelaneousTestRecords(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    testRecordList.add(new TestRecord(optionalPrefix + "gif" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "jpg" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "pdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "png" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "tif" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "zip" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "text" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "html" + optionalSuffix,MimeTypeDisposition.ACCEPT_HTML,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "xml" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "pdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "txt" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "unknown" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "x-httpd-php" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "x-mapp-php" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "x-mapp-php4" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));

    testRecordList.add(new TestRecord(optionalPrefix + "message/news" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "message/rfc822" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "multipart/form-data" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "multipart/x-mixed-replace" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));

    testRecordList.add(new TestRecord(optionalPrefix + "plain/text" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "x-application/pdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "attachment/pdf" + optionalSuffix,MimeTypeDisposition.ACCEPT_TEXT,overrideDisposition));

    testRecordList.add(new TestRecord(optionalPrefix + "model/iges" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "model/vnd.dwf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "model/vrml" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "NULL" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "octet-stream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "octet/stream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));

    testRecordList.add(new TestRecord(optionalPrefix + "plugin/x-theorist" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "reader/x-lit" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "server-parsed" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "true-type/font" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "www/unknown" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "x-epoc/x-sisx-app" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "x-type/subtype" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "x-world/x-vrml" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "xml/octet" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "binary/octec-stream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "binary/octet-stream" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "file/compress" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));


    testRecordList.add(new TestRecord(optionalPrefix + "chemical/x-cerius" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "chemical/x-chemdraw" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "chemical/x-mdl-molfile" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "chemical/x-mdl-tgf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "chemical/x-molconn-Z" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "chemical/x-mopac-input" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "chemical/x-pdb" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    testRecordList.add(new TestRecord(optionalPrefix + "drawing/x-dwf" + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));    
  }
  
  static void addInvalidExtensions(LinkedList<TestRecord> testRecordList,String optionalPrefix,String optionalSuffix,MimeTypeDisposition overrideDisposition) { 
    for (String invalidExtension : invalidExtensions) { 
      testRecordList.add(new TestRecord(optionalPrefix + invalidExtension + optionalSuffix,MimeTypeDisposition.REJECT,overrideDisposition));
    }
  }
  
  public static void main(String[] args) {
    
    LinkedList<TestRecord> testRecords = new LinkedList<TestRecord>();
    
    addBinaryTestRecords(testRecords, "", "",null);
    addValidTextTypeRecords(testRecords, "", "",null);
    addValidHTMLTypeRecords(testRecords, "", "",null);
    addOtherValidTextTypeRecords(testRecords, "", "",null);
    addTestApplicationStrings(testRecords,"", "",null);
    addAudioTestRecords(testRecords, "", "",null);
    addImageTestRecords(testRecords, "", "",null);
    addTestTextRecords(testRecords, "", "",null);
    addTestVideoRecords(testRecords, "", "",null);
    addMiscelaneousTestRecords(testRecords, "", "",null);
    addInvalidExtensions(testRecords,"","",null);
    
    addBinaryTestRecords(testRecords, "", " ; charset: UTF-8;charset=iso-8859-1",null);
    addValidTextTypeRecords(testRecords, "", " ; charset: UTF-8;charset=iso-8859-1",null);
    addValidHTMLTypeRecords(testRecords, "", " ; charset: UTF-8;charset=iso-8859-1",null);
    addOtherValidTextTypeRecords(testRecords, "", " ; charset: UTF-8;charset=iso-8859-1",null);
    addOtherValidTextTypeRecords(testRecords, ".", " ; charset: UTF-8;charset=iso-8859-1",null);
    addTestApplicationStrings(testRecords,"", " ; charset: UTF-8;charset=iso-8859-1",null);
    addAudioTestRecords(testRecords, "", " ; charset: UTF-8;charset=iso-8859-1",null);
    addImageTestRecords(testRecords, "", " ; charset: UTF-8;charset=iso-8859-1",null);
    addTestTextRecords(testRecords, "", " ; charset: UTF-8;charset=iso-8859-1",null);
    addTestVideoRecords(testRecords, "", " ; charset: UTF-8;charset=iso-8859-1",null);
    addMiscelaneousTestRecords(testRecords, "", " ; charset: UTF-8;charset=iso-8859-1",null);
    addInvalidExtensions(testRecords,"."," ; charset: UTF-8;charset=iso-8859-1",null);
    
    for (TestRecord record : testRecords) { 
      MimeTypeDisposition disposition = checkMimeTypeDisposition(record._testString);
      if (disposition != record._expectedDisposition) { 
        System.out.println("RECORD:" + record._testString + " Expected:" + record._expectedDisposition + " GOT:" + disposition);
      }
    }
    
  }

}
