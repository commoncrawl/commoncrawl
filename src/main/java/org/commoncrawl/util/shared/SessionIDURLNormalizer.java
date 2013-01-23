package org.commoncrawl.util.shared;

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


import java.net.MalformedURLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Remove some common session id patterns from incoming urls
 * 
 * @author rana 
 */

public class SessionIDURLNormalizer {

  // ^(.*php.*)/osCsid/([0-9a-fA-F]*)$ ->
  // http://www.bearcountryuk.com/index.php/cPath/50/teddy+bear+name/Accessories/osCsid/4d4b2659aa5f1a39d907d315cf0a5209
  // ^(.*php.*)/([0-9a-fA-F]*)$ ->
  // http://www.myredpacket.co.uk/section.php/25/12/birthday-gifts-and-presents/d1b4c32d834a331b63109589ef730c27
  // ^(.*)/PHPSESSID/([0-9a-fA-F]*).html$ ->
  // http://www.minshuku-web.com/catalog/869/PHPSESSID/96bc0d2490b3ce6206d04c1ed7ccfb26.html
  // ^(.*/)sessions/[0-9a-fA-F]*/(.*)$ ->
  // http://ifshinviolins.com/sessions/dd603a0a691faeb744db3f72212ca888/store
  // ^(.*);\$sessionid\$([0-9a-zA-Z]*)$ ->
  // http://www.iexplore.co.uk/travel-photos/French+Polynesia/1;$sessionid$BHZYH4QAAMGH5TBKYHVCFEQ
  // ^(.*)/session_id/([0-9a-zA-Z]*)(.*)$ ->
  // http://www.reinke.com/index.html/session_id/d606e74935a60c04d9989082b2fb624d/screen/interesting_links
  // ^(.*)--session_id.[0-9a-zA-Z]*(.*)$ ->
  // http://www.iboats.com/Portable_Above_Deck_Fuel_Tanks/dm/cart_id.726334728--category_id.238165--search_type.category--session_id.729524783--view_id.238165

  private static Pattern pattern0           = Pattern.compile("^[0-9a-fA-F]*$");               // http://www.bearcountryuk.com/index.php/cPath/50/teddy+bear+name/Accessories/osCsid/4d4b2659aa5f1a39d907d315cf0a5209
  private static Pattern pattern0_1         = Pattern.compile("[0-9a-zA-Z]*");
  private static Pattern pattern0_2         = Pattern.compile("[0-9]*");
  private static Pattern pattern0_3         = Pattern
                                                .compile("^[0-9a-fA-F\\-]*$");
  private static Pattern pattern1           = Pattern
                                                .compile("^(.*php.*)/osCsid/[0-9a-fA-F]*$");   // http://www.bearcountryuk.com/index.php/cPath/50/teddy+bear+name/Accessories/osCsid/4d4b2659aa5f1a39d907d315cf0a5209
  private static Pattern pattern2           = Pattern
                                                .compile("^(.*php.*)/[0-9a-fA-F]*$");          // http://www.myredpacket.co.uk/section.php/25/12/birthday-gifts-and-presents/d1b4c32d834a331b63109589ef730c27
  private static Pattern pattern3           = Pattern
                                                .compile("^(.*)/PHPSESSID.[0-9a-fA-F]*.*$");   // http://www.minshuku-web.com/catalog/869/PHPSESSID/96bc0d2490b3ce6206d04c1ed7ccfb26.html
  private static Pattern pattern4           = Pattern
                                                .compile("^(.*/)sessions/[0-9a-fA-F]*/(.*)$"); // http://ifshinviolins.com/sessions/dd603a0a691faeb744db3f72212ca888/store
  private static Pattern pattern5           = Pattern
                                                .compile("^(.*);\\$sessionid\\$[0-9a-zA-Z]*$"); // http://www.iexplore.co.uk/travel-photos/French+Polynesia/1;$sessionid$BHZYH4QAAMGH5TBKYHVCFEQ
  private static Pattern pattern6           = Pattern
                                                .compile("^(.*)/session_id/[0-9a-zA-Z]*(.*)$"); // http://www.reinke.com/index.html/session_id/d606e74935a60c04d9989082b2fb624d/screen/interesting_links
  private static Pattern pattern7           = Pattern
                                                .compile("^(.*)--session_id.[0-9]*(.*)$");     // http://www.iboats.com/Portable_Above_Deck_Fuel_Tanks/dm/cart_id.726334728--category_id.238165--search_type.category--session_id.729524783--view_id.238165

  private Configuration  config;

  private static String  OSCSID             = "oscsid";
  private static String  PHPSESSID          = "phpsessid";
  private static String  OSCSID_W_DASH      = "-oscsid-";

  private static String  SESSIONS           = "/sessions/";
  private static String  $SESSIONS$         = ";$sessionid$";
  private static String  SESSION_ID         = "/session_id/";
  private static String  DASHDASH_SESSIONID = "--session_id.";
  private static String  JSESSIONID         = ";jsessionid=";
  private static String  SID                = "sid";
  private static String  MSCSID             = "mscsid";

  public String normalize(String urlString, String scope)
      throws MalformedURLException {

    String urlStringOriginal = urlString;
    urlString = urlString.toLowerCase();

    {
      int phpsessidIDX = urlString.lastIndexOf(PHPSESSID);

      if (phpsessidIDX != -1) {
        int charPosAfterPHPSessId = phpsessidIDX + PHPSESSID.length();

        if (urlString.length() > charPosAfterPHPSessId) {
          char charAfterPHPSessId = urlString.charAt(charPosAfterPHPSessId);
          if (charAfterPHPSessId == '=' || charAfterPHPSessId == '.'
              || charAfterPHPSessId == '-' || charAfterPHPSessId == '+'
              || charAfterPHPSessId == '/') {
            int idStart = charPosAfterPHPSessId + 1;
            int idEnd = idStart;
            while (idEnd != urlString.length()) {
              if (urlString.charAt(idEnd) == '&'
                  || urlString.charAt(idEnd) == '.'
                  || urlString.charAt(idEnd) == '-'
                  || urlString.charAt(idEnd) == '+'
                  || urlString.charAt(idEnd) == '/')
                break;
              ++idEnd;
            }
            String idStr = urlString.substring(idStart, idEnd);

            if (idStr.length() != 0) {
              Matcher m = pattern0.matcher(idStr);
              int desiredLength = 32;
              if (!m.matches()) {
                m = pattern0_1.matcher(idStr);
                desiredLength = 26;
              }
              if (idStr.length() == desiredLength && m.matches()) {
                if (idEnd == urlString.length()) {
                  return urlStringOriginal.substring(0, phpsessidIDX - 1);
                } else {
                  return urlStringOriginal.substring(0, phpsessidIDX - 1)
                      + urlStringOriginal.substring(idEnd);
                }
              }
            }
          }
        }
      }
    }

    if (urlString.indexOf(".php") != -1) {
      int lastSlashPos = urlString.lastIndexOf('/');
      if (lastSlashPos != -1) {
        String idStr = urlString.substring(lastSlashPos + 1);
        if (idStr.length() == 32) {
          Matcher m = pattern0.matcher(idStr);
          if (m.matches()) {
            int nextToLastSlashIndex = urlString.lastIndexOf('/',
                lastSlashPos - 1);
            if (nextToLastSlashIndex != -1) {
              if (urlString.indexOf("oscsid", nextToLastSlashIndex + 1) == nextToLastSlashIndex + 1) {
                return urlStringOriginal.substring(0, nextToLastSlashIndex);
              }
              /*
               * else { return urlStringOriginal.substring(0,lastSlashPos); }
               */
            }
          }
        }
      }
    }

    int indexOfOSCSID = urlString.lastIndexOf(OSCSID);
    if (indexOfOSCSID != -1) {
      int indexOfNextSlash = urlString.indexOf('/', indexOfOSCSID
          + OSCSID.length());
      if (indexOfNextSlash != -1) {
        String idStr = urlString.substring(indexOfNextSlash + 1);
        if (idStr.length() == 32) {
          Matcher m = pattern0.matcher(idStr);
          if (m.matches()) {
            return urlStringOriginal.substring(0, indexOfOSCSID - 1);
          }
        }
      }
    }

    int oscsidDashIDX = urlString.lastIndexOf(OSCSID_W_DASH);
    if (oscsidDashIDX != -1) {
      int dotHTMLIDX = urlString.lastIndexOf(".html");
      if (dotHTMLIDX > oscsidDashIDX) {
        String idStr = urlString.substring(oscsidDashIDX
            + OSCSID_W_DASH.length(), dotHTMLIDX);
        if (idStr.length() == 32) {
          Matcher m = pattern0.matcher(idStr);
          if (m.matches()) {
            return urlStringOriginal.substring(0, oscsidDashIDX)
                + urlStringOriginal.substring(dotHTMLIDX);
          }
        }
      }
    }

    int sessionsIDX = urlString.indexOf(SESSIONS);
    if (sessionsIDX != -1) {
      int nextSlashIDX = urlString
          .indexOf('/', sessionsIDX + SESSIONS.length());
      if (nextSlashIDX != -1) {
        String idStr = urlString.substring(sessionsIDX + SESSIONS.length(),
            nextSlashIDX);
        if (idStr.length() == 32) {
          Matcher m = pattern0.matcher(idStr);
          if (m.matches()) {
            return urlStringOriginal.substring(0, sessionsIDX)
                + urlStringOriginal.substring(nextSlashIDX);
          }
        }
      }
    }

    int dollarSessionIDX = urlString.indexOf($SESSIONS$);

    if (dollarSessionIDX != -1) {
      Matcher m = pattern0_1.matcher(urlString.substring(dollarSessionIDX
          + $SESSIONS$.length()));
      if (m.matches()) {
        return urlStringOriginal.substring(0, dollarSessionIDX);
      }
    }

    int session_id_IDX = urlString.indexOf(SESSION_ID);
    if (session_id_IDX != -1) {
      int nextSlashIDX = urlString.indexOf('/', session_id_IDX
          + SESSION_ID.length());
      if (nextSlashIDX != -1) {
        String idStr = urlString.substring(
            session_id_IDX + SESSION_ID.length(), nextSlashIDX);
        if (idStr.length() == 32) {
          Matcher m = pattern0.matcher(idStr);
          if (m.matches()) {
            return urlStringOriginal.substring(0, session_id_IDX)
                + urlStringOriginal.substring(nextSlashIDX);
          }
        }
      }
    }

    int dashdashIDX = urlString.indexOf(DASHDASH_SESSIONID);
    if (dashdashIDX != -1) {
      int nextDashDashIDX = urlString.indexOf("--", dashdashIDX
          + DASHDASH_SESSIONID.length());
      if (nextDashDashIDX != -1) {
        Matcher m = pattern0_2.matcher(urlString.substring(dashdashIDX
            + DASHDASH_SESSIONID.length(), nextDashDashIDX));
        if (m.matches()) {
          return urlStringOriginal.substring(0, dashdashIDX)
              + urlStringOriginal.substring(nextDashDashIDX);
        }
      }
    }

    {
      String matchingStr = null;
      int sidIDX = urlString.lastIndexOf(SID);
      if (sidIDX != -1 && sidIDX != 0) {
        if (urlString.charAt(sidIDX - 1) == '/'
            || urlString.charAt(sidIDX - 1) == '?'
            || urlString.charAt(sidIDX - 1) == '&'
            || urlString.charAt(sidIDX - 1) == '+') {
          matchingStr = SID;
        }
      }
      if (matchingStr == null) {
        sidIDX = urlString.lastIndexOf(OSCSID);
        if (sidIDX != -1 && sidIDX != 0) {
          if (urlString.charAt(sidIDX - 1) == '/'
              || urlString.charAt(sidIDX - 1) == '?'
              || urlString.charAt(sidIDX - 1) == '&'
              || urlString.charAt(sidIDX - 1) == '+') {
            matchingStr = OSCSID;
          }
        }
      }
      if (matchingStr == null) {
        sidIDX = urlString.lastIndexOf(MSCSID);
        if (sidIDX != -1 && sidIDX != 0) {
          if (urlString.charAt(sidIDX - 1) == '/'
              || urlString.charAt(sidIDX - 1) == '?'
              || urlString.charAt(sidIDX - 1) == '&'
              || urlString.charAt(sidIDX - 1) == '+') {
            matchingStr = MSCSID;
          }
        }
      }

      if (matchingStr != null) {
        {
          int charPosAfterSessId = sidIDX + matchingStr.length();
          if (urlString.length() > charPosAfterSessId) {
            char charAfterSessId = urlString.charAt(charPosAfterSessId);
            if (charAfterSessId == '=' || charAfterSessId == '.'
                || charAfterSessId == '-' || charAfterSessId == '+'
                || charAfterSessId == '/') {
              int idStart = charPosAfterSessId + 1;
              int idEnd = idStart;
              while (idEnd != urlString.length()) {
                if (urlString.charAt(idEnd) == '&'
                    || urlString.charAt(idEnd) == '.'
                    || urlString.charAt(idEnd) == '+'
                    || urlString.charAt(idEnd) == '/')
                  break;
                ++idEnd;
              }
              String idStr = urlString.substring(idStart, idEnd);

              if (idStr.length() != 0) {
                int desiredLength = 32;
                Matcher m = pattern0.matcher(idStr);
                if (!m.matches()) {
                  m = pattern0_3.matcher(idStr);
                  desiredLength = 36; // with dashes ....
                }
                if (!m.matches()) {
                  m = pattern0_1.matcher(idStr);
                  desiredLength = 26; // with dashes ....
                }

                if (m.matches() && idStr.length() >= desiredLength) {
                  if (idEnd == urlString.length()) {
                    return urlStringOriginal.substring(0, sidIDX - 1);
                  } else {
                    return urlStringOriginal.substring(0, sidIDX - 1)
                        + urlStringOriginal.substring(idEnd);
                  }
                }
              }
            }
          }
        }
      }

    }

    int jsessionIdIDX = urlString.indexOf(JSESSIONID);

    if (jsessionIdIDX != -1) {
      // find trailing delimiter (if any)
      int indexOfQuery = urlString.indexOf('?', jsessionIdIDX);

      if (indexOfQuery != -1) {
        return urlStringOriginal.substring(0, jsessionIdIDX)
            + urlStringOriginal.substring(indexOfQuery);
      } else {
        return urlStringOriginal.substring(0, jsessionIdIDX);
      }
    }

    return urlStringOriginal;
  }

  public Configuration getConf() {
    return config;
  }

  public void setConf(Configuration conf) {
    config = conf;
  }

  static String testStrings[] = {
      "http://www.bearcountryuk.com/images/bc0059.jpg/osCsid/96a7bddc9c8a4249dbabd862f859e9e1",
      "http://www.jileyes.com/lingerie_category-cat-26-name-Inseparables___ensembles__soutien_gorge-osCsid-3416a5c31a2013e37cf87ca963c6c99f.html",

      "http://www.construfacil.com/index.php/P/search/PHPSESSID/015c350a9dcead350788459fe27e1d2c", // *
      "http://www.didglobal.com/page/PHPSESSID/db2efa56f2d298cbed0f27be2574cbfe/home",
      "http://www.lot-tissimo.com/zf/1/PHPSESSID/gfhte7m6riss8a57kt8hou7bl6/",
      "http://www.droles-blagues.com/news+index.storytopic+0+start+10+PHPSESSID+dabb2d0c754e989167997c0f6cca69b3.htm",
      "http://relax-navi.net/formmail+index.id_form+1+PHPSESSID+8014724e439c07d12e0bb63599af99e1.htm",
      "http://www.tagtag.com/site/mobile/terms/PHPSESSID/a82av7cnicjak8t8gcq9ss8lg6",
      "http://www.nblskil.org/ct/wffaq+index.PHPSESSID+7f1426a7e7d6f8717a05028335811b9e.htm",
      "http://www.soft-news.net/m-news+index+PHPSESSID-7375c6f2abc8237cefb6a19012281821.html",
      "http://www.horizon-etudiant.com/news+index.PHPSESSID+df70913950e6a2aeca5049f6ccbf2a46.htm",

      "http://www.classicsilks.com/catalog/images//osCsid/1eccdf955e1accf18372a3e12aa92fd6",
      "http://www.bearcountryuk.com/index.php/cPath/50/teddy+bear+name/Accessories/osCsid/4d4b2659aa5f1a39d907d315cf0a5209",
      "http://www.myredpacket.co.uk/section.php/25/12/birthday-gifts-and-presents/d1b4c32d834a331b63109589ef730c27",
      "http://www.minshuku-web.com/catalog/869/PHPSESSID/96bc0d2490b3ce6206d04c1ed7ccfb26.html",
      "http://ifshinviolins.com/sessions/dd603a0a691faeb744db3f72212ca888/store",
      "http://www.iexplore.co.uk/travel-photos/French+Polynesia/1;$sessionid$BHZYH4QAAMGH5TBKYHVCFEQ",
      "http://www.reinke.com/index.html/session_id/d606e74935a60c04d9989082b2fb624d/screen/interesting_links",
      "http://www.iboats.com/Portable_Above_Deck_Fuel_Tanks/dm/cart_id.726334728--category_id.238165--search_type.category--session_id.729524783--view_id.238165",
      "http://quote.yahoo.com/tech-ticker/article/37053/VMware-Tanks-as-CEO-Greene-Gets-Ousted;_ylt=An1dUveIfo30T0EBvyw6_US7YWsA?tickers=vmw",
      "https://www.harrahs.com/AvailabilityCalendar.do?propCode=PLV",
      "http://www.google.com/search?hl=en&q=st+jude+hospital+fullerton&btnG=Google+Search",
      "http://www.bearcountryuk.com/index.ddd/cPath/50/teddy+bear+name/Accessories/osCsid/4d4b2659aa5f1a39d907d315cf0a5209",
      "http://www.bearcountryuk.com/index.ddd;jsessionid=08301521611089820628281",
      "http://www.myredpacket.co.uk/section.php/25/12/birthday-gifts-and-presents;JSESSIONID=08301521611089820628281",
      "http://www1.cimaglobal.com/cps/rde/xchg/SID-0AE7C4D1-E388165B/live/root.xsl/13928.htm",
      "http://www.placidway.com/treatment-detail/20/Orthopedic/Knee-Surgery-Treatment-Abroad//?PHPSESSID=c83e4440fdb325634206cda3482aa758",
      "http://www.allacademic.com/one/www/www/index.php?cmd=www&PHPSESSID=e563c9711d20c906de543d52a1633072",
      "http://boards.bootsnall.com/the-team.html?sid=f52964b93dcfeb6a9ba43b0caf44d752",
      "http://www.fnac.com/livre.asp?SID=2f3f0314-8164-f087-e7e9-4ed9487391c8&UID=0B3FF5542-5944-146B-8EEB-ECDB3218C6AF&Origin=FnacAff&OrderInSession=0&TTL=040520100324&bl=2%5b1pro%5dliv",
      "http://forums-test.mozillazine.org/memberlist.php?mode=viewprofile&u=261941&sid=dd4c61187cd950ad4b64b8e4da7c20a9",
      "http://www.rainbowresource.com/prodlist.php?sid=1257592724-171162",
      "http://www.eloan.com/s/show/glossary?context=refi&lockdays=30&sid=B456E0E99B62D31EAB4274D8B59B944A&user=&mcode=&vid=",
      "http://www.motherwear.com/cs/sizechart.cfm?cid=107&sid=25046",
      "http://www.trainpetdog.com/store/terms-of-use.php?osCsid=b27eecba862e5c723c05b2f4245c06ea",
      "http://alumni.byu.edu/s/1085/03-provo-Alumni/index.aspx?sid=1085&gid=7&pgid=60&cid=169&referer=&query=emeriti%2fpdf%2femeritiwinter09.pdf",
      "http://www.couponchief.com/coupons/submit?sid=4422",
      "http://www.emeraldinsight.com/Insight/menuNavigation.do;jsessionid=A17FC93E864C2F8B3709F63558BA69DB?hdAction=InsightHome",
      "http://www.lakeshorelearning.com/order/onlineOrder.jsp;jsessionid=KxMMpRGgPpC1ktZ1pJJCZF1MmmFxZHPnyrNJhBmWJGHkhcL5Hd4p!-617247554!NONE?FOLDER%3C%3Efolder_id=2534374302096766&ASSORTMENT%3C%3East_id=1408474395181113&bmUID=1257311436941" };

  @Test
  public void unitTest() throws Exception {

    long totalTimeStart = System.currentTimeMillis();
    for (String url : testStrings) {

      long nanoSecsStart = System.nanoTime();
      String result = normalize(url, "");
      long nanoSecsEnd = System.nanoTime();
      long nanoTime;

      if (nanoSecsEnd < nanoSecsStart) {
        nanoTime = (Long.MAX_VALUE - nanoSecsStart) + nanoSecsEnd;
      } else {
        nanoTime = nanoSecsEnd - nanoSecsStart;
      }
      if (result != url) {
        System.out.print("*");
      }
      System.out.println("Time:" + nanoTime + "Source:" + url + " Resolved to:"
          + result);
    }

    long totalTimeEnd = System.currentTimeMillis();
    System.out.println("Total Time:" + (totalTimeEnd - totalTimeStart));

  }

  public static void main(String[] args) {
    try {
      new SessionIDURLNormalizer().unitTest();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
