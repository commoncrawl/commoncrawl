package org.commoncrawl.util.shared;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;

/**
 * Pseudo flexible HTTP date parser
 * 
 * @author rana
 * 
 */
public class DateUtils {

  public static class DateParser {

    SimpleDateFormat parsers[] = null;

    public DateParser(String[] patterns) {
      parsers = new SimpleDateFormat[patterns.length];
      int index = 0;
      for (String pattern : patterns) {
        parsers[index++] = new SimpleDateFormat(pattern);
      }
    }

    public Date parseDate(String str) throws ParseException {
      if (str == null) {
        throw new IllegalArgumentException("Date and Patterns must not be null");
      }

      ParsePosition pos = new ParsePosition(0);

      for (SimpleDateFormat parser : parsers) {

        Date date = parser.parse(str, pos);
        pos.setIndex(0);

        if (date != null && pos.getIndex() == str.length()) {
          return date;
        }
      }

      throw new ParseException("Unable to parse the date: " + str, -1);
    }
  }

  static String kMonths[]   = { "jan", "feb", "mar", "apr", "may", "jun",
      "jul", "aug", "sep", "oct", "nov", "dec" };

  static String kDelimiters = "\t !\"#$%&'()*+,-./;<=>?@[\\]^_`{|}~";

  static class TimeExploded {
    int year;        // Four digit year "2007"
    int month;       // 1-based month (values 1 = January, etc.)
    int day_of_week; // 0-based day of week (0 = Sunday, etc.)
    int day_of_month; // 1-based day of month (1-31)
    int hour;        // Hour within the current day (0-23)
    int minute;      // Minute within the current hour (0-59)
    int second;      // Second within the current minute (0-59 plus leap
    // seconds which may take it up to 60).
    int millisecond; // Milliseconds within the current second (0-999)
  }

  static Pattern timePattern = Pattern.compile("(\\d+):(\\d+):(\\d+).*");

  static boolean isASCIIDigit(char c) {
    return (c >= '0') & (c <= '9');
  }

  // Parse a cookie expiration time. We try to be lenient, but we need to
  // assume some order to distinguish the fields. The basic rules:
  // - The month name must be present and prefix the first 3 letters of the
  // full month name (jan for January, jun for June).
  // - If the year is <= 2 digits, it must occur after the day of month.
  // - The time must be of the format hh:mm:ss.
  // An average cookie expiration will look something like this:
  // Sat, 15-Apr-17 21:01:22 GMT
  public static long parseHttpDate(String time_string) {

    int kMonthsLen = kMonths.length;
    // We want to be pretty liberal, and support most non-ascii and non-digit
    // characters as a delimiter. We can't treat : as a delimiter, because it
    // is the delimiter for hh:mm:ss, and we want to keep this field together.
    // We make sure to include - and +, since they could prefix numbers.
    // If the cookie attribute came in in quotes (ex expires="XXX"), the quotes
    // will be preserved, and we will get them here. So we make sure to include
    // quote characters, and also \ for anything that was internally escaped.

    TimeExploded exploded = new TimeExploded();

    StringTokenizer tokenizer = new StringTokenizer(time_string, kDelimiters);

    boolean found_day_of_month = false;
    boolean found_month = false;
    boolean found_time = false;
    boolean found_year = false;

    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken();
      boolean numerical = isASCIIDigit(token.charAt(0));

      // String field
      if (!numerical) {
        if (!found_month) {
          String tokenLowerCase = token.toLowerCase();
          for (int i = 0; i < kMonthsLen; ++i) {
            // Match prefix, so we could match January, etc
            if (tokenLowerCase.startsWith(kMonths[i])) {
              exploded.month = i + 1;
              found_month = true;
              break;
            }
          }
        } else {
          // If we've gotten here, it means we've already found and parsed our
          // month, and we have another string, which we would expect to be the
          // the time zone name. According to the RFC and my experiments with
          // how sites format their expirations, we don't have much of a reason
          // to support timezones. We don't want to ever barf on user input,
          // but this DCHECK should pass for well-formed data.
          // DCHECK(token == "GMT");
        }
        // Numeric field w/ a colon
      } else if (token.indexOf(':') != -1) {
        if (!found_time) {
          Matcher m = timePattern.matcher(token);
          if (m.matches()) {
            try {
              short hour = Short.parseShort(m.group(1));
              short minute = Short.parseShort(m.group(2));
              short second = Short.parseShort(m.group(3));

              exploded.hour = hour;
              exploded.minute = minute;
              exploded.second = second;
              found_time = true;
            } catch (NumberFormatException e) {

            }
          }
        } else {
          // We should only ever encounter one time-like thing. If we're here,
          // it means we've found a second, which shouldn't happen. We keep
          // the first. This check should be ok for well-formed input:
          // NOTREACHED();
        }
        // Numeric field
      } else {
        // Overflow with atoi() is unspecified, so we enforce a max length.
        if (!found_day_of_month && token.length() <= 2) {
          try {
            exploded.day_of_month = Integer.parseInt(token);
            found_day_of_month = true;
          } catch (NumberFormatException e) {

          }
        } else if (!found_year && token.length() <= 5) {
          try {
            exploded.year = Integer.parseInt(token);
            found_year = true;
          } catch (NumberFormatException e) {

          }
        } else {
          // If we're here, it means we've either found an extra numeric field,
          // or a numeric field which was too long. For well-formed input, the
          // following check would be reasonable:
          // NOTREACHED();
        }
      }
    }

    if (!found_day_of_month || !found_month || !found_time || !found_year) {
      // We didn't find all of the fields we need. For well-formed input, the
      // following check would be reasonable:
      // NOTREACHED() << "Cookie parse expiration failed: " << time_string;
      return -1;
    }

    // Normalize the year to expand abbreviated years to the full year.
    if (exploded.year >= 69 && exploded.year <= 99)
      exploded.year += 1900;
    if (exploded.year >= 0 && exploded.year <= 68)
      exploded.year += 2000;

    // If our values are within their correct ranges, we got our time.
    if (exploded.day_of_month >= 1 && exploded.day_of_month <= 31
        && exploded.month >= 1 && exploded.month <= 12 && exploded.year >= 1601
        && exploded.year <= 30827 && exploded.hour <= 23
        && exploded.minute <= 59 && exploded.second <= 59) {

      Calendar gmtCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

      gmtCalendar.set(exploded.year, exploded.month - 1, exploded.day_of_month,
          exploded.hour, exploded.minute, exploded.second);
      gmtCalendar.set(Calendar.MILLISECOND, 0);
      return gmtCalendar.getTimeInMillis();
    }

    // One of our values was out of expected range. For well-formed input,
    // the following check would be reasonable:
    // NOTREACHED() << "Cookie exploded expiration failed: " << time_string;

    return -1;
  }

  public static void main(String[] args) {
    Assert.assertFalse(parseHttpDate("Sun, 22 Nov 2009 01:37:06GMT") == -1);
    Assert.assertFalse(parseHttpDate("Sun, 22 Nov 2009 01:37:06 GMT") == -1);

  }
}
