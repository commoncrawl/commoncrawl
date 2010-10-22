package org.commoncrawl.util.shared;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.commoncrawl.io.shared.NIOHttpHeaders;
import org.commoncrawl.util.shared.HttpHeaderUtils.ContentTypeAndCharset;
import org.mozilla.intl.chardet.nsDetector;
import org.mozilla.intl.chardet.nsICharsetDetectionObserver;
import org.mozilla.intl.chardet.nsPSMDetector;

import com.google.common.collect.ImmutableMap;

/**
 * 
 * Charset detection and decoding helpers
 * 
 * @author rana
 *
 */
public class CharsetUtils {

	public static final Log LOG = LogFactory.getLog(CharsetUtils.class);
	
	/** check for a normalized (java friendly) alias for the original charset name **/
	public static String aliasCharset(String inputCharset) { 
		String alias = aliasTable.get(inputCharset.toLowerCase());
		return (alias != null) ? alias : inputCharset;
	}
	
	public static String bestEffortDecodeBytes(String headers,byte[] crawlData)throws IOException { 
		
		ContentTypeAndCharset urlMetadata = new ContentTypeAndCharset();
		
    // parse final header result and populate appropriate metadata fields 
    NIOHttpHeaders finalHeaders = NIOHttpHeaders.parseHttpHeaders(headers);
    HttpHeaderUtils.parseContentType(finalHeaders, urlMetadata);
		
		
		
    if (urlMetadata._charset != null && crawlData != null) {
      if (urlMetadata._contentType != null && urlMetadata._contentType.equalsIgnoreCase("text/html")) { 
        // sniff encoding in metadata ...
        String alternateCharset = CharsetUtils.sniffCharacterEncoding(crawlData);
        if (alternateCharset != null) {
          urlMetadata._charset  = alternateCharset;
        }
      }
    }
    
    // now if charset is REALLY still not available ... 
    if (urlMetadata._charset == null && crawlData != null) {
    	
      if (urlMetadata._contentType != null && MimeTypeFilter.isValidTextType(urlMetadata._contentType)) {
        // try to detect the charset from the stream ... 
        String detectedCharset = CharsetUtils.detetechCharacterEncoding(crawlData);
        
        if (detectedCharset != null) { 
          urlMetadata._charset = (detectedCharset);
        }
        else {
          // manually set charset to ASCII :-( 
          urlMetadata._charset = ("ASCII");
        }
      }
    }

    // finally if charset is available ... try to alias it ... 
    if (urlMetadata._charset != null) { 
      String aliasValue = aliasTable.get(urlMetadata._charset.toLowerCase());
      if (aliasValue != null) { 
        urlMetadata._charset = aliasValue;
      }
    }
    
    // now try to find it via java names
    if (urlMetadata._charset != null && crawlData != null) { 
      Charset charset = null;
      try { 
        charset = Charset.forName(urlMetadata._charset);
      }
      catch (Exception e) { 
        LOG.error(CCStringUtils.stringifyException(e));
        // try to detect the charset from the stream ... 
        String detectedCharset = CharsetUtils.detetechCharacterEncoding(crawlData);
        if (detectedCharset != null) { 
          try { 
            charset = Charset.forName(detectedCharset);
          }
          catch (Exception e2) { 
            LOG.error(CCStringUtils.stringifyException(e));
          }
        }
      }
      if (charset != null) { 
        try { 
          CharBuffer ucs2Chars = charset.decode(ByteBuffer.wrap(crawlData));
          return ucs2Chars.toString();
        }
        catch (Exception e) { 
          LOG.error(CCStringUtils.stringifyException(e));
        }
      }
    }
    else { 
      LOG.error("NULL_CHARSET_PASSED_TO_PARSER");
    }

    return null;		
	}
	
  private static final int CHUNK_SIZE = 2000;

  private static Pattern metaPattern =
    Pattern.compile("<meta\\s+([^>]*http-equiv=\"?content-type\"?[^>]*)>",
                    Pattern.CASE_INSENSITIVE);
  private static Pattern charsetPattern =
    Pattern.compile("charset=\\s*([a-z][_\\-0-9a-z]*)",
                    Pattern.CASE_INSENSITIVE);
  
  /**
   * Given a <code>byte[]</code> representing an html file of an 
   * <em>unknown</em> encoding,  read out 'charset' parameter in the meta tag   
   * from the first <code>CHUNK_SIZE</code> bytes.
   * If there's no meta tag for Content-Type or no charset is specified,
   * <code>null</code> is returned.  <br />
   * FIXME: non-byte oriented character encodings (UTF-16, UTF-32)
   * can't be handled with this. 
   * We need to do something similar to what's done by mozilla
   * (http://lxr.mozilla.org/seamonkey/source/parser/htmlparser/src/nsParser.cpp#1993).
   * See also http://www.w3.org/TR/REC-xml/#sec-guessing
   * <br />
   *
   * @param content <code>byte[]</code> representation of an html file
   */

  public static String sniffCharacterEncoding(byte[] content) {
    
    // LOG.info("ENTERING SNIFFCHARENCODING...");
    int length = content.length < CHUNK_SIZE ? 
                 content.length : CHUNK_SIZE;

    // We don't care about non-ASCII parts so that it's sufficient
    // to just inflate each byte to a 16-bit value by padding. 
    // For instance, the sequence {0x41, 0x82, 0xb7} will be turned into 
    // {U+0041, U+0082, U+00B7}. 
    String str = "";
    try {
      str = new String(content, 0, length,
                       Charset.forName("ASCII").toString());
    } catch (UnsupportedEncodingException e) {
      // code should never come here, but just in case... 
      return null;
    }

    //LOG.info("RUNNING METAPATTERN MATCHER...");
    Matcher metaMatcher = metaPattern.matcher(str);
    String encoding = null;
    
    if (metaMatcher.find()) {
      //LOG.info("RUNNING CHARSET PATTERN MATCHER...");
      Matcher charsetMatcher = charsetPattern.matcher(metaMatcher.group(1));
      if (charsetMatcher.find()) 
        encoding = new String(charsetMatcher.group(1));
    }

    //LOG.info("ENCODING IS:" + encoding);
    return encoding;
  }
  
  public static class DetectorState implements nsICharsetDetectionObserver {

    public boolean _done =false;
    public String  _detectedCharset = null;
    
    @Override
    public void Notify(String charset) {
      _detectedCharset = charset;
      _done = true;
    } 
    
  }
  
  private static int MAX_CHARS_TO_DETECT = 16000;
  
  /** last resort - detect encoding using charset detector **/
  public static String detetechCharacterEncoding(byte[] content) { 
    
    if (content != null && content.length != 0) { 
    

      DetectorState state = new DetectorState();

      nsDetector detector = new nsDetector(nsPSMDetector.ALL) ;
      
      detector.Init(state);

      boolean isAscii = detector.isAscii(content, content.length);
      
      if (!isAscii) { 
        isAscii = detector.DoIt(content,Math.min(content.length,MAX_CHARS_TO_DETECT),false);
      }
      detector.DataEnd();
      
      if (isAscii) {
        return "ASCII";
      }
      else if (state._detectedCharset != null) { 
        return state._detectedCharset;
      } else { 
        String prob[] = detector.getProbableCharsets();
        if (prob != null && prob.length != 0) { 
          return prob[0];
        }
      }
    }
    return null;
  }
  
  static final ImmutableMap<String, String> aliasTable; 
  
  
  static { 
    
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>();
  
    builder.put("ibm-1208","UTF-8");
    builder.put("ibm-1209","UTF-8");
    builder.put("ibm-5304","UTF-8");
    builder.put("ibm-5305","UTF-8");
    builder.put("ibm-13496","UTF-8");
    builder.put("ibm-13497","UTF-8");
    builder.put("ibm-17592","UTF-8");
    builder.put("ibm-17593","UTF-8");
    builder.put("windows-65001","UTF-8");
    builder.put("cp1208","UTF-8");
    builder.put("iso-10646-ucs-2","UTF-16");
    builder.put("ibm-1204","UTF-16");
    builder.put("ibm-1205","UTF-16");
    builder.put("unicode","UTF-16");
    builder.put("csunicode","UTF-16");
    builder.put("ucs-2","UTF-16");
    builder.put("x-utf-16be","UTF-16BE");
    builder.put("unicodebigunmarked","UTF-16BE");
    builder.put("ibm-1200","UTF-16BE");
    builder.put("ibm-1201","UTF-16BE");
    builder.put("ibm-13488","UTF-16BE");
    builder.put("ibm-13489","UTF-16BE");
    builder.put("ibm-17584","UTF-16BE");
    builder.put("ibm-17585","UTF-16BE");
    builder.put("ibm-21680","UTF-16BE");
    builder.put("ibm-21681","UTF-16BE");
    builder.put("ibm-25776","UTF-16BE");
    builder.put("ibm-25777","UTF-16BE");
    builder.put("ibm-29872","UTF-16BE");
    builder.put("ibm-29873","UTF-16BE");
    builder.put("ibm-61955","UTF-16BE");
    builder.put("ibm-61956","UTF-16BE");
    builder.put("windows-1201","UTF-16BE");
    builder.put("cp1200","UTF-16BE");
    builder.put("cp1201","UTF-16BE");
    builder.put("utf16_bigendian","UTF-16BE");
    builder.put("x-utf-16le","UTF-16LE");
    builder.put("unicodelittleunmarked","UTF-16LE");
    builder.put("ibm-1202","UTF-16LE");
    builder.put("ibm-1203","UTF-16LE");
    builder.put("ibm-13490","UTF-16LE");
    builder.put("ibm-13491","UTF-16LE");
    builder.put("ibm-17586","UTF-16LE");
    builder.put("ibm-17587","UTF-16LE");
    builder.put("ibm-21682","UTF-16LE");
    builder.put("ibm-21683","UTF-16LE");
    builder.put("ibm-25778","UTF-16LE");
    builder.put("ibm-25779","UTF-16LE");
    builder.put("ibm-29874","UTF-16LE");
    builder.put("ibm-29875","UTF-16LE");
    builder.put("utf16_littleendian","UTF-16LE");
    builder.put("windows-1200","UTF-16LE");
    builder.put("ibm-819","ISO-8859-1");
    builder.put("ibm819","ISO-8859-1");
    builder.put("cp819","ISO-8859-1");
    builder.put("latin1","ISO-8859-1");
    builder.put("8859_1","ISO-8859-1");
    builder.put("csisolatin1","ISO-8859-1");
    builder.put("iso-ir-100","ISO-8859-1");
    builder.put("iso_8859-1:1987","ISO-8859-1");
    builder.put("l1","ISO-8859-1");
    builder.put("819","ISO-8859-1");
    builder.put("ascii","US-ASCII");
    builder.put("ansi_x3.4-1968","US-ASCII");
    builder.put("ansi_x3.4-1986","US-ASCII");
    builder.put("iso_646.irv:1991","US-ASCII");
    builder.put("iso_646.irv:1983","US-ASCII");
    builder.put("iso646-us","US-ASCII");
    builder.put("us","US-ASCII");
    builder.put("csascii","US-ASCII");
    builder.put("iso-ir-6","US-ASCII");
    builder.put("cp367","US-ASCII");
    builder.put("ascii7","US-ASCII");
    builder.put("646","US-ASCII");
    builder.put("windows-20127","US-ASCII");
    builder.put("ibm-367","US-ASCII");
    builder.put("ibm367","US-ASCII");
    builder.put("ibm-912_p100-1995","ISO-8859-2");
    builder.put("ibm-912","ISO-8859-2");
    builder.put("iso_8859-2:1987","ISO-8859-2");
    builder.put("latin2","ISO-8859-2");
    builder.put("csisolatin2","ISO-8859-2");
    builder.put("iso-ir-101","ISO-8859-2");
    builder.put("l2","ISO-8859-2");
    builder.put("8859_2","ISO-8859-2");
    builder.put("cp912","ISO-8859-2");
    builder.put("912","ISO-8859-2");
    builder.put("windows-28592","ISO-8859-2");
    builder.put("ibm-913_p100-2000","ISO-8859-3");
    builder.put("ibm-913","ISO-8859-3");
    builder.put("iso_8859-3:1988","ISO-8859-3");
    builder.put("latin3","ISO-8859-3");
    builder.put("csisolatin3","ISO-8859-3");
    builder.put("iso-ir-109","ISO-8859-3");
    builder.put("l3","ISO-8859-3");
    builder.put("8859_3","ISO-8859-3");
    builder.put("cp913","ISO-8859-3");
    builder.put("913","ISO-8859-3");
    builder.put("windows-28593","ISO-8859-3");
    builder.put("ibm-914_p100-1995","ISO-8859-4");
    builder.put("ibm-914","ISO-8859-4");
    builder.put("latin4","ISO-8859-4");
    builder.put("csisolatin4","ISO-8859-4");
    builder.put("iso-ir-110","ISO-8859-4");
    builder.put("iso_8859-4:1988","ISO-8859-4");
    builder.put("l4","ISO-8859-4");
    builder.put("8859_4","ISO-8859-4");
    builder.put("cp914","ISO-8859-4");
    builder.put("914","ISO-8859-4");
    builder.put("windows-28594","ISO-8859-4");
    builder.put("ibm-915_p100-1995","ISO-8859-5");
    builder.put("ibm-915","ISO-8859-5");
    builder.put("cyrillic","ISO-8859-5");
    builder.put("csisolatincyrillic","ISO-8859-5");
    builder.put("iso-ir-144","ISO-8859-5");
    builder.put("iso_8859-5:1988","ISO-8859-5");
    builder.put("8859_5","ISO-8859-5");
    builder.put("cp915","ISO-8859-5");
    builder.put("915","ISO-8859-5");
    builder.put("windows-28595","ISO-8859-5");
    builder.put("ibm-1089_p100-1995","ISO-8859-6");
    builder.put("ibm-1089","ISO-8859-6");
    builder.put("arabic","ISO-8859-6");
    builder.put("csisolatinarabic","ISO-8859-6");
    builder.put("iso-ir-127","ISO-8859-6");
    builder.put("iso_8859-6:1987","ISO-8859-6");
    builder.put("ecma-114","ISO-8859-6");
    builder.put("asmo-708","ISO-8859-6");
    builder.put("8859_6","ISO-8859-6");
    builder.put("cp1089","ISO-8859-6");
    builder.put("1089","ISO-8859-6");
    builder.put("windows-28596","ISO-8859-6");
    builder.put("iso-8859-6-i","ISO-8859-6");
    builder.put("iso-8859-6-e","ISO-8859-6");
    builder.put("ibm-9005_x110-2007","ISO-8859-7");
    builder.put("ibm-9005","ISO-8859-7");
    builder.put("greek","ISO-8859-7");
    builder.put("greek8","ISO-8859-7");
    builder.put("elot_928","ISO-8859-7");
    builder.put("ecma-118","ISO-8859-7");
    builder.put("csisolatingreek","ISO-8859-7");
    builder.put("iso-ir-126","ISO-8859-7");
    builder.put("iso_8859-7:1987","ISO-8859-7");
    builder.put("windows-28597","ISO-8859-7");
    builder.put("sun_eu_greek","ISO-8859-7");
    builder.put("ibm-813_p100-1995","ISO-8859-7");
    builder.put("ibm-813","ISO-8859-7");
    builder.put("8859_7","ISO-8859-7");
    builder.put("cp813","ISO-8859-7");
    builder.put("813","ISO-8859-7");
    builder.put("ibm-5012_p100-1999","ISO-8859-8");
    builder.put("ibm-5012","ISO-8859-8");
    builder.put("hebrew","ISO-8859-8");
    builder.put("csisolatinhebrew","ISO-8859-8");
    builder.put("iso-ir-138","ISO-8859-8");
    builder.put("iso_8859-8:1988","ISO-8859-8");
    builder.put("iso-8859-8-i","ISO-8859-8");
    builder.put("iso-8859-8-e","ISO-8859-8");
    builder.put("8859_8","ISO-8859-8");
    builder.put("windows-28598","ISO-8859-8");
    builder.put("hebrew8","ISO-8859-8");
    builder.put("ibm-916_p100-1995","ibm-916");
    builder.put("cp916","ibm-916");
    builder.put("916","ibm-916");
    builder.put("ibm-920_p100-1995","ISO-8859-9");
    builder.put("ibm-920","ISO-8859-9");
    builder.put("latin5","ISO-8859-9");
    builder.put("csisolatin5","ISO-8859-9");
    builder.put("iso-ir-148","ISO-8859-9");
    builder.put("iso_8859-9:1989","ISO-8859-9");
    builder.put("l5","ISO-8859-9");
    builder.put("8859_9","ISO-8859-9");
    builder.put("cp920","ISO-8859-9");
    builder.put("920","ISO-8859-9");
    builder.put("windows-28599","ISO-8859-9");
    builder.put("ecma-128","ISO-8859-9");
    builder.put("turkish8","ISO-8859-9");
    builder.put("turkish","ISO-8859-9");
    builder.put("ibm-921_p100-1995","ISO-8859-13");
    builder.put("ibm-921","ISO-8859-13");
    builder.put("8859_13","ISO-8859-13");
    builder.put("windows-28603","ISO-8859-13");
    builder.put("cp921","ISO-8859-13");
    builder.put("921","ISO-8859-13");
    builder.put("ibm-923_p100-1998","ISO-8859-15");
    builder.put("ibm-923","ISO-8859-15");
    builder.put("latin-9","ISO-8859-15");
    builder.put("l9","ISO-8859-15");
    builder.put("8859_15","ISO-8859-15");
    builder.put("latin0","ISO-8859-15");
    builder.put("csisolatin0","ISO-8859-15");
    builder.put("csisolatin9","ISO-8859-15");
    builder.put("iso8859_15_fdis","ISO-8859-15");
    builder.put("cp923","ISO-8859-15");
    builder.put("923","ISO-8859-15");
    builder.put("windows-28605","ISO-8859-15");
    builder.put("ibm-943_p15a-2003","Shift_JIS");
    builder.put("ms_kanji","Shift_JIS");
    builder.put("csshiftjis","Shift_JIS");
    builder.put("windows-31j","Shift_JIS");
    builder.put("cswindows31j","Shift_JIS");
    builder.put("x-sjis","Shift_JIS");
    builder.put("x-ms-cp932","Shift_JIS");
    builder.put("cp932","Shift_JIS");
    builder.put("windows-932","Shift_JIS");
    builder.put("cp943c","Shift_JIS");
    builder.put("ibm-943c","Shift_JIS");
    builder.put("ms932","Shift_JIS");
    builder.put("pck","Shift_JIS");
    builder.put("sjis","Shift_JIS");
    builder.put("s-jis","Shift_JIS");
    builder.put("ibm-943_vsub_vpua","Shift_JIS");
    builder.put("ibm-943_p130-1999","x-IBM943");
    builder.put("ibm-943","x-IBM943");
    builder.put("shift_jis","x-IBM943");
    builder.put("943","x-IBM943");
    builder.put("ibm-943_vascii_vsub_vpu","x-IBM943");
    builder.put("cp943","x-IBM943");
    builder.put("ibm-33722_p120-1999","x-IBM33722");
    builder.put("ibm-5050","x-IBM33722");
    builder.put("cp33722","x-IBM33722");
    builder.put("33722","x-IBM33722");
    builder.put("ibm-33722_vascii_vpua","x-IBM33722");
    builder.put("ibm-954_p101-2007","x-JISAutoDetect");
    builder.put("ibm-954","x-JISAutoDetect");
    builder.put("euc-jp","x-JISAutoDetect");
    builder.put("cseucpkdfmtjapanese","x-JISAutoDetect");
    builder.put("x-euc-jp","x-JISAutoDetect");
    builder.put("eucjis","x-JISAutoDetect");
    builder.put("ujis","x-JISAutoDetect");
    builder.put("windows-950-2000","Big5");
    builder.put("csbig5","Big5");
    builder.put("windows-950","Big5");
    builder.put("x-big5","Big5");
    builder.put("ibm-950_p110-1999","x-IBM950");
    builder.put("ibm-950","x-IBM950");
    builder.put("cp950","x-IBM950");
    builder.put("950","x-IBM950");
    builder.put("ibm-1375_p100-2007","Big5-HKSCS");
    builder.put("ibm-1375","Big5-HKSCS");
    builder.put("big5-hkscs","Big5-HKSCS");
    builder.put("big5hk","Big5-HKSCS");
    builder.put("hkscs-big5","Big5-HKSCS");
    builder.put("ibm-5471_p100-2006","x-MS950-HKSCS");
    builder.put("ibm-5471","x-MS950-HKSCS");
    builder.put("ms950_hkscs","x-MS950-HKSCS");
    builder.put("hkbig5","x-MS950-HKSCS");
    builder.put("big5-hkscs:unicode3.0","x-MS950-HKSCS");
    builder.put("windows-936-2000","GBK");
    builder.put("cp936","GBK");
    builder.put("ms936","GBK");
    builder.put("windows-936","GBK");
    builder.put("ibm-1383_p110-1999","GB2312");
    builder.put("ibm-1383","GB2312");
    builder.put("csgb2312","GB2312");
    builder.put("cp1383","GB2312");
    builder.put("1383","GB2312");
    builder.put("euc-cn","GB2312");
    builder.put("ibm-euccn","GB2312");
    builder.put("hp15cn","GB2312");
    builder.put("ibm-1383_vpua","GB2312");
    builder.put("ibm-964_p110-1999","x-IBM964");
    builder.put("ibm-964","x-IBM964");
    builder.put("euc-tw","x-IBM964");
    builder.put("ibm-euctw","x-IBM964");
    builder.put("cns11643","x-IBM964");
    builder.put("cp964","x-IBM964");
    builder.put("964","x-IBM964");
    builder.put("ibm-964_vpua","x-IBM964");
    builder.put("ibm-949_p110-1999","x-IBM949");
    builder.put("ibm-949","x-IBM949");
    builder.put("cp949","x-IBM949");
    builder.put("949","x-IBM949");
    builder.put("ibm-949_vascii_vsub_vpua","x-IBM949");
    builder.put("ibm-970_p110_p110-2006_u2","EUC-KR");
    builder.put("ibm-970","EUC-KR");
    builder.put("euc-kr","EUC-KR");
    builder.put("ks_c_5601-1987","EUC-KR");
    builder.put("windows-51949","EUC-KR");
    builder.put("cseuckr","EUC-KR");
    builder.put("ibm-euckr","EUC-KR");
    builder.put("ksc_5601","EUC-KR");
    builder.put("5601","EUC-KR");
    builder.put("cp970","EUC-KR");
    builder.put("970","EUC-KR");
    builder.put("ibm-970_vpua","EUC-KR");
    builder.put("windows-949-2000","x-windows-949");
    builder.put("windows-949","x-windows-949");
    builder.put("ks_c_5601-1989","x-windows-949");
    builder.put("csksc56011987","x-windows-949");
    builder.put("korean","x-windows-949");
    builder.put("iso-ir-149","x-windows-949");
    builder.put("ms949","x-windows-949");
    builder.put("windows-874-2000","x-windows-874");
    builder.put("windows-874","x-windows-874");
    builder.put("ms874","x-windows-874");
    builder.put("ibm-874_p100-1995","x-IBM874");
    builder.put("ibm-874","x-IBM874");
    builder.put("ibm-9066","x-IBM874");
    builder.put("cp874","x-IBM874");
    builder.put("tis-620","x-IBM874");
    builder.put("tis620.2533","x-IBM874");
    builder.put("eucth","x-IBM874");
    builder.put("ibm-437_p100-1995","IBM437");
    builder.put("ibm437","IBM437");
    builder.put("cp437","IBM437");
    builder.put("437","IBM437");
    builder.put("cspc8codepage437","IBM437");
    builder.put("windows-437","IBM437");
    builder.put("ibm-737_p100-1997","x-IBM737");
    builder.put("ibm-737","x-IBM737");
    builder.put("ibm737","x-IBM737");
    builder.put("cp737","x-IBM737");
    builder.put("windows-737","x-IBM737");
    builder.put("737","x-IBM737");
    builder.put("ibm-775_p100-1996","IBM775");
    builder.put("ibm-775","IBM775");
    builder.put("ibm775","IBM775");
    builder.put("cp775","IBM775");
    builder.put("cspc775baltic","IBM775");
    builder.put("windows-775","IBM775");
    builder.put("775","IBM775");
    builder.put("ibm-850_p100-1995","IBM850");
    builder.put("ibm-850","IBM850");
    builder.put("ibm850","IBM850");
    builder.put("cp850","IBM850");
    builder.put("850","IBM850");
    builder.put("cspc850multilingual","IBM850");
    builder.put("windows-850","IBM850");
    builder.put("ibm-852_p100-1995","IBM852");
    builder.put("ibm-852","IBM852");
    builder.put("ibm852","IBM852");
    builder.put("cp852","IBM852");
    builder.put("852","IBM852");
    builder.put("cspcp852","IBM852");
    builder.put("windows-852","IBM852");
    builder.put("ibm-855_p100-1995","IBM855");
    builder.put("ibm-855","IBM855");
    builder.put("ibm855","IBM855");
    builder.put("cp855","IBM855");
    builder.put("855","IBM855");
    builder.put("csibm855","IBM855");
    builder.put("cspcp855","IBM855");
    builder.put("windows-855","IBM855");
    builder.put("ibm-856_p100-1995","x-IBM856");
    builder.put("ibm-856","x-IBM856");
    builder.put("ibm856","x-IBM856");
    builder.put("cp856","x-IBM856");
    builder.put("856","x-IBM856");
    builder.put("ibm-857_p100-1995","IBM857");
    builder.put("ibm-857","IBM857");
    builder.put("cp857","IBM857");
    builder.put("857","IBM857");
    builder.put("csibm857","IBM857");
    builder.put("windows-857","IBM857");
    builder.put("ibm-858_p100-1997","IBM00858");
    builder.put("ibm-858","IBM00858");
    builder.put("ccsid00858","IBM00858");
    builder.put("cp00858","IBM00858");
    builder.put("pc-multilingual-850+euro","IBM00858");
    builder.put("cp858","IBM00858");
    builder.put("windows-858","IBM00858");
    builder.put("ibm-860_p100-1995","IBM860");
    builder.put("ibm-860","IBM860");
    builder.put("cp860","IBM860");
    builder.put("860","IBM860");
    builder.put("csibm860","IBM860");
    builder.put("ibm-861_p100-1995","IBM861");
    builder.put("ibm-861","IBM861");
    builder.put("cp861","IBM861");
    builder.put("861","IBM861");
    builder.put("cp-is","IBM861");
    builder.put("csibm861","IBM861");
    builder.put("windows-861","IBM861");
    builder.put("ibm-862_p100-1995","IBM862");
    builder.put("ibm-862","IBM862");
    builder.put("cp862","IBM862");
    builder.put("862","IBM862");
    builder.put("cspc862latinhebrew","IBM862");
    builder.put("dos-862","IBM862");
    builder.put("windows-862","IBM862");
    builder.put("ibm-863_p100-1995","");
    builder.put("ibm-863","IBM863");
    builder.put("cp863","IBM863");
    builder.put("863","IBM863");
    builder.put("csibm863","IBM863");
    builder.put("ibm-864_x110-1999","IBM864");
    builder.put("ibm-864","IBM864");
    builder.put("cp864","IBM864");
    builder.put("csibm864","IBM864");
    builder.put("ibm-865_p100-1995","IBM865");
    builder.put("ibm-865","IBM865");
    builder.put("cp865","IBM865");
    builder.put("865","IBM865");
    builder.put("csibm865","IBM865");
    builder.put("ibm-866_p100-1995","IBM866");
    builder.put("ibm-866","IBM866");
    builder.put("cp866","IBM866");
    builder.put("866","IBM866");
    builder.put("csibm866","IBM866");
    builder.put("windows-866","IBM866");
    builder.put("ibm-868_p100-1995","IBM868");
    builder.put("ibm-868","IBM868");
    builder.put("cp868","IBM868");
    builder.put("868","IBM868");
    builder.put("csibm868","IBM868");
    builder.put("cp-ar","IBM868");
    builder.put("ibm-869_p100-1995","IBM869");
    builder.put("ibm-869","IBM869");
    builder.put("cp869","IBM869");
    builder.put("869","IBM869");
    builder.put("cp-gr","IBM869");
    builder.put("csibm869","IBM869");
    builder.put("windows-869","IBM869");
    builder.put("ibm-878_p100-1996","KOI8-R");
    builder.put("ibm-878","KOI8-R");
    builder.put("koi8-r","KOI8-R");
    builder.put("koi8","KOI8-R");
    builder.put("cskoi8r","KOI8-R");
    builder.put("windows-20866","KOI8-R");
    builder.put("cp878","KOI8-R");
    builder.put("ibm-922_p100-1999","x-IBM922");
    builder.put("ibm-922","x-IBM922");
    builder.put("ibm922","x-IBM922");
    builder.put("cp922","x-IBM922");
    builder.put("922","x-IBM922");
    builder.put("ibm-5346_p100-1998","windows-1250");
    builder.put("ibm-5346","windows-1250");
    builder.put("cp1250","windows-1250");
    builder.put("ibm-5347_p100-1998","windows-1251");
    builder.put("ibm-5347","windows-1251");
    builder.put("cp1251","windows-1251");
    builder.put("ansi1251","windows-1251");
    builder.put("ibm-5348_p100-1997","windows-1252");
    builder.put("ibm-5348","windows-1252");
    builder.put("cp1252","windows-1252");
    builder.put("ibm-5349_p100-1998","windows-1253");
    builder.put("ibm-5349","windows-1253");
    builder.put("cp1253","windows-1253");
    builder.put("ibm-5350_p100-1998","windows-1254");
    builder.put("ibm-5350","windows-1254");
    builder.put("cp1254","windows-1254");
    builder.put("ibm-9447_p100-2002","windows-1255");
    builder.put("ibm-9447","windows-1255");
    builder.put("cp1255","windows-1255");
    builder.put("ibm-9448_x100-2005","windows-1256");
    builder.put("ibm-9448","windows-1256");
    builder.put("cp1256","windows-1256");
    builder.put("ibm-9449_p100-2002","windows-1257");
    builder.put("ibm-9449","windows-1257");
    builder.put("cp1257","windows-1257");
    builder.put("ibm-5354_p100-1998","windows-1258");
    builder.put("ibm-5354","windows-1258");
    builder.put("cp1258","windows-1258");
    builder.put("ibm-1006_p100-1995","x-IBM1006");
    builder.put("ibm-1006","x-IBM1006");
    builder.put("ibm1006","x-IBM1006");
    builder.put("cp1006","x-IBM1006");
    builder.put("1006","x-IBM1006");
    builder.put("ibm-1098_p100-1995","x-IBM1006");
    builder.put("ibm-1098","x-IBM1006");
    builder.put("ibm1098","x-IBM1006");
    builder.put("cp1098","x-IBM1006");
    builder.put("1098","x-IBM1006");
    builder.put("ibm-1124_p100-1996","x-IBM1124");
    builder.put("ibm-1124","x-IBM1124");
    builder.put("cp1124","x-IBM1124");
    builder.put("1124","x-IBM1124");
    builder.put("ISO_2022,locale=ja,version=0","ISO-2022-JP");
    builder.put("iso-2022-jp","ISO-2022-JP");
    builder.put("csiso2022jp","ISO-2022-JP");
    builder.put("ISO_2022,locale=ko,version=0","ISO-2022-KR");
    builder.put("iso-2022-kr","ISO-2022-KR");
    builder.put("csiso2022kr","ISO-2022-KR");
    builder.put("ISO_2022,locale=zh,version=0","ISO-2022-CN");
    builder.put("iso-2022-cn","ISO-2022-CN");
    builder.put("csiso2022cn","ISO-2022-CN");
    builder.put("ibm-37_p100-1995","IBM037");
    builder.put("ibm-37","IBM037");
    builder.put("ibm-037","IBM037");
    builder.put("ebcdic-cp-us","IBM037");
    builder.put("ebcdic-cp-ca","IBM037");
    builder.put("ebcdic-cp-wt","IBM037");
    builder.put("ebcdic-cp-nl","IBM037");
    builder.put("csibm037","IBM037");
    builder.put("cp037","IBM037");
    builder.put("37","IBM037");
    builder.put("cpibm37","IBM037");
    builder.put("cp37","IBM037");
    builder.put("ibm-273_p100-1995","IBM273");
    builder.put("ibm-273","IBM273");
    builder.put("cp273","IBM273");
    builder.put("csibm273","IBM273");
    builder.put("ebcdic-de","IBM273");
    builder.put("273","IBM273");
    builder.put("ibm-277_p100-1995","IBM277");
    builder.put("ibm-277","IBM277");
    builder.put("cp277","IBM277");
    builder.put("ebcdic-cp-dk","IBM277");
    builder.put("ebcdic-cp-no","IBM277");
    builder.put("csibm277","IBM277");
    builder.put("ebcdic-dk","IBM277");
    builder.put("277","IBM277");
    builder.put("ibm-278_p100-1995","IBM278");
    builder.put("ibm-278","IBM278");
    builder.put("cp278","IBM278");
    builder.put("ebcdic-cp-fi","IBM278");
    builder.put("ebcdic-cp-se","IBM278");
    builder.put("csibm278","IBM278");
    builder.put("ebcdic-sv","IBM278");
    builder.put("278","IBM278");
    builder.put("ibm-280_p100-1995","IBM280");
    builder.put("ibm-280","IBM280");
    builder.put("cp280","IBM280");
    builder.put("ebcdic-cp-it","IBM280");
    builder.put("csibm280","IBM280");
    builder.put("280","IBM280");
    builder.put("ibm-284_p100-1995","IBM284");
    builder.put("ibm-284","IBM284");
    builder.put("cp284","IBM284");
    builder.put("ebcdic-cp-es","IBM284");
    builder.put("csibm284","IBM284");
    builder.put("cpibm284","IBM284");
    builder.put("284","IBM284");
    builder.put("ibm-285_p100-1995","IBM285");
    builder.put("ibm-285","IBM285");
    builder.put("cp285","IBM285");
    builder.put("ebcdic-cp-gb","IBM285");
    builder.put("csibm285","IBM285");
    builder.put("cpibm285","IBM285");
    builder.put("ebcdic-gb","IBM285");
    builder.put("285","IBM285");
    builder.put("ibm-297_p100-1995","IBM297");
    builder.put("ibm-297","IBM297");
    builder.put("cp297","IBM297");
    builder.put("ebcdic-cp-fr","IBM297");
    builder.put("csibm297","IBM297");
    builder.put("cpibm297","IBM297");
    builder.put("297","IBM297");
    builder.put("ibm-420_x120-1999","IBM420");
    builder.put("ibm-420","IBM420");
    builder.put("ibm420","IBM420");
    builder.put("cp420","IBM420");
    builder.put("ebcdic-cp-ar1","IBM420");
    builder.put("csibm420","IBM420");
    builder.put("420","IBM420");
    builder.put("ibm-424_p100-1995","IBM424");
    builder.put("ibm-424","IBM424");
    builder.put("cp424","IBM424");
    builder.put("ebcdic-cp-he","IBM424");
    builder.put("csibm424","IBM424");
    builder.put("424","IBM424");
    builder.put("ibm-500_p100-1995","IBM500");
    builder.put("ibm-500","IBM500");
    builder.put("cp500","IBM500");
    builder.put("ebcdic-cp-be","IBM500");
    builder.put("csibm500","IBM500");
    builder.put("ebcdic-cp-ch","IBM500");
    builder.put("500","IBM500");
    builder.put("ibm-838_p100-1995","IBM-Thai");
    builder.put("ibm-838","IBM-Thai");
    builder.put("ibm838","IBM-Thai");
    builder.put("csibmthai","IBM-Thai");
    builder.put("cp838","IBM-Thai");
    builder.put("838","IBM-Thai");
    builder.put("ibm-9030","IBM-Thai");
    builder.put("ibm-870_p100-1995","IBM870");
    builder.put("ibm-870","IBM870");
    builder.put("cp870","IBM870");
    builder.put("ebcdic-cp-roece","IBM870");
    builder.put("ebcdic-cp-yu","IBM870");
    builder.put("csibm870","IBM870");
    builder.put("ibm-871_p100-1995","IBM871");
    builder.put("ibm-871","IBM871");
    builder.put("ebcdic-cp-is","IBM871");
    builder.put("csibm871","IBM871");
    builder.put("cp871","IBM871");
    builder.put("ebcdic-is","IBM871");
    builder.put("871","IBM871");
    builder.put("ibm-875_p100-1995","x-IBM875");
    builder.put("ibm-875","x-IBM875");
    builder.put("ibm875","x-IBM875");
    builder.put("cp875","x-IBM875");
    builder.put("875","x-IBM875");
    builder.put("ibm-918_p100-1995","IBM918");
    builder.put("ibm-918","IBM918");
    builder.put("cp918","IBM918");
    builder.put("ebcdic-cp-ar2","IBM918");
    builder.put("csibm918","IBM918");
    builder.put("ibm-930_p120-1999","x-IBM930");
    builder.put("ibm-930","x-IBM930");
    builder.put("ibm-5026","x-IBM930");
    builder.put("ibm930","x-IBM930");
    builder.put("cp930","x-IBM930");
    builder.put("930","x-IBM930");
    builder.put("ibm-933_p110-1995","x-IBM933");
    builder.put("ibm-933","x-IBM933");
    builder.put("cp933","x-IBM933");
    builder.put("933","x-IBM933");
    builder.put("ibm-935_p110-1999","x-IBM935");
    builder.put("ibm-935","x-IBM935");
    builder.put("cp935","x-IBM935");
    builder.put("935","x-IBM935");
    builder.put("ibm-937_p110-1999","x-IBM937");
    builder.put("ibm-937","x-IBM937");
    builder.put("cp937","x-IBM937");
    builder.put("937","x-IBM937");
    builder.put("ibm-939_p120-1999","x-IBM939");
    builder.put("ibm-939","x-IBM939");
    builder.put("ibm-931","x-IBM939");
    builder.put("ibm-5035","x-IBM939");
    builder.put("ibm939","x-IBM939");
    builder.put("cp939","x-IBM939");
    builder.put("939","x-IBM939");
    builder.put("ibm-1025_p100-1995","x-IBM1025");
    builder.put("ibm-1025","x-IBM1025");
    builder.put("cp1025","x-IBM1025");
    builder.put("1025","x-IBM1025");
    builder.put("ibm-1026_p100-1995","IBM1026");
    builder.put("ibm-1026","IBM1026");
    builder.put("ibm1026","IBM1026");
    builder.put("cp1026","IBM1026");
    builder.put("csibm1026","IBM1026");
    builder.put("1026","IBM1026");
    builder.put("ibm-1047_p100-1995","IBM1047");
    builder.put("ibm-1047","IBM1047");
    builder.put("ibm1047","IBM1047");
    builder.put("cp1047","IBM1047");
    builder.put("1047","IBM1047");
    builder.put("ibm-1097_p100-1995","x-IBM1097");
    builder.put("ibm-1097","x-IBM1097");
    builder.put("cp1097","x-IBM1097");
    builder.put("1097","x-IBM1097");
    builder.put("ibm-1112_p100-1995","x-IBM1112");
    builder.put("ibm-1112","x-IBM1112");
    builder.put("cp1112","x-IBM1112");
    builder.put("1112","x-IBM1112");
    builder.put("ibm-1122_p100-1999","x-IBM1122");
    builder.put("ibm-1122","x-IBM1122");
    builder.put("cp1122","x-IBM1122");
    builder.put("1122","x-IBM1122");
    builder.put("ibm-1123_p100-1995","x-IBM1123");
    builder.put("ibm-1123","x-IBM1123");
    builder.put("cp1123","x-IBM1123");
    builder.put("1123","x-IBM1123");
    builder.put("ibm-1140_p100-1997","IBM01140");
    builder.put("ibm-1140","IBM01140");
    builder.put("ccsid01140","IBM01140");
    builder.put("cp01140","IBM01140");
    builder.put("cp1140","IBM01140");
    builder.put("ebcdic-us-37+euro","IBM01140");
    builder.put("ibm-1141_p100-1997","IBM01141");
    builder.put("ibm-1141","IBM01141");
    builder.put("ccsid01141","IBM01141");
    builder.put("cp01141","IBM01141");
    builder.put("cp1141","IBM01141");
    builder.put("ebcdic-de-273+euro","IBM01141");
    builder.put("ibm-1142_p100-1997","IBM01142");
    builder.put("ibm-1142","IBM01142");
    builder.put("ccsid01142","IBM01142");
    builder.put("cp01142","IBM01142");
    builder.put("cp1142","IBM01142");
    builder.put("ebcdic-dk-277+euro","IBM01142");
    builder.put("ebcdic-no-277+euro","IBM01142");
    builder.put("ibm-1143_p100-1997","IBM01143");
    builder.put("ibm-1143","IBM01143");
    builder.put("ccsid01143","IBM01143");
    builder.put("cp01143","IBM01143");
    builder.put("cp1143","IBM01143");
    builder.put("ebcdic-fi-278+euro","IBM01143");
    builder.put("ebcdic-se-278+euro","IBM01143");
    builder.put("ibm-1144_p100-1997","IBM01144");
    builder.put("ibm-1144","IBM01144");
    builder.put("ccsid01144","IBM01144");
    builder.put("cp01144","IBM01144");
    builder.put("cp1144","IBM01144");
    builder.put("ebcdic-it-280+euro","IBM01144");
    builder.put("ibm-1145_p100-1997","IBM01145");
    builder.put("ibm-1145","IBM01145");
    builder.put("ccsid01145","IBM01145");
    builder.put("cp01145","IBM01145");
    builder.put("cp1145","IBM01145");
    builder.put("ebcdic-es-284+euro","IBM01145");
    builder.put("ibm-1146_p100-1997","IBM01146");
    builder.put("ibm-1146","IBM01146");
    builder.put("ccsid01146","IBM01146");
    builder.put("cp01146","IBM01146");
    builder.put("cp1146","IBM01146");
    builder.put("ebcdic-gb-285+euro","IBM01146");
    builder.put("ibm-1147_p100-1997","IBM01147");
    builder.put("ibm-1147","IBM01147");
    builder.put("ccsid01147","IBM01147");
    builder.put("cp01147","IBM01147");
    builder.put("cp1147","IBM01147");
    builder.put("ebcdic-fr-297+euro","IBM01147");
    builder.put("ibm-1148_p100-1997","IBM01148");
    builder.put("ibm-1148","IBM01148");
    builder.put("ccsid01148","IBM01148");
    builder.put("cp01148","IBM01148");
    builder.put("cp1148","IBM01148");
    builder.put("ebcdic-international-500+euro","IBM01148");
    builder.put("ibm-1149_p100-1997","IBM01149");
    builder.put("ibm-1149","IBM01149");
    builder.put("ccsid01149","IBM01149");
    builder.put("cp01149","IBM01149");
    builder.put("cp1149","IBM01149");
    builder.put("ebcdic-is-871+euro","IBM01149");
    aliasTable = builder.build();
  }
  
}
