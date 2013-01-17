package org.commoncrawl.util.shared;

import java.net.MalformedURLException;
import java.util.Collection;
import java.util.regex.Pattern;

import org.commoncrawl.protocol.shared.URLFPV2;
import org.commoncrawl.util.shared.GoogleURL;
import org.commoncrawl.util.shared.URLFingerprint;

public class URLUtils {

  /**
   * Internal Helper
   * 
   * @param candidateString
   * @param parts
   * @param rootNameIndex
   * @return
   */
  private static String buildRootNameString(String candidateString,
      String[] parts, int rootNameIndex) {
    int partsToInclude = parts.length - rootNameIndex;
    int dotsToInclude = partsToInclude - 1;

    // initial root name length is dot count
    int rootNameLength = dotsToInclude;
    for (int i = rootNameIndex; i < parts.length; ++i) {
      rootNameLength += parts[i].length();
    }
    return candidateString.substring(candidateString.length() - rootNameLength);
  }

  /**
   * Extract Top-Level Domain Name given Host Name
   * 
   * @param hostName
   *          - domain name
   * @return tld name or null if invalid domain name
   */
  public static String extractTLDName(String hostName) {

    // special case for ip addresses
    if (ipAddressRegEx.matcher(hostName).matches()) {
      return "inaddr-arpa.arpa";
    }

    if (hostName.endsWith(".")) {
      hostName = hostName.substring(0, hostName.length() - 1);
    }
    if (hostName.startsWith("*") && hostName.length() > 1) {
      hostName = hostName.substring(1);
    }
    if (hostName.length() != 0) {
      if (!invalidDomainCharactersRegEx.matcher(hostName).find()) {
        String parts[] = hostName.split("\\.");
        if (parts.length >= 2) {
          Collection<String> secondaryNames = TLDNamesCollection
              .getSecondaryNames(parts[parts.length - 1]);

          if (secondaryNames.size() != 0) {
            // see if second to last part matches secondary names for this TLD
            // or there is a wildcard expression for secondary name in rule set
            if (secondaryNames.contains(parts[parts.length - 2])
                || secondaryNames.contains("*")) {
              // ok secondary part is potentianlly part of secondary name ...

              // check to see the part in not explicitly excluded ...
              if (secondaryNames.contains("!" + parts[parts.length - 2])) {
                // in this case, second to last part is NOT part of secondary
                // name
                return buildRootNameString(hostName, parts, parts.length - 1);
              } else {
                // otherwise, TLD contains 2 parts
                return buildRootNameString(hostName, parts, parts.length - 2);
              }
            }
            // ok second to last part does not match set of known secondary
            // names
            else {
              // make a wildcard string matching secondary name
              String extendedWildcard = "*." + parts[parts.length - 2];
              // if match, then this implies secondary name has two components
              if (secondaryNames.contains(extendedWildcard)) {

                if (parts.length >= 3) {
                  // this implies that there must be four parts to the name to
                  // extract root
                  // unless exlusion rule applies
                  String exclusionRule2 = "!" + parts[parts.length - 3] + "."
                      + parts[parts.length - 2];

                  // if exclusion rule is present ...
                  if (secondaryNames.contains(exclusionRule2)) {
                    // third part is NOT part of secondary name
                    return buildRootNameString(hostName, parts,
                        parts.length - 2);
                  } else {
                    // ok extended wildcard matched. last 3 parts are part of
                    // the TLD
                    if (parts.length >= 4) {
                      return buildRootNameString(hostName, parts,
                          parts.length - 3);
                    }
                  }
                }
              }
              // at this point ... if the null name exists ...
              else if (secondaryNames.contains("")) {
                // only last item is part of TLD
                return buildRootNameString(hostName, parts, parts.length - 1);
              }
            }
          }
        }
      }
    }
    return null;
  }

  /**
   * Extract the Root Domain Name (domain name up to one name after TLD Name)
   * from a fully qualified domain name
   * 
   * @param hostName
   *          fully Qualified Domain Name
   * @return Root Domain Name or null if domain name is not valid
   */
  public static String extractRootDomainName(String hostName) {

    // special case for ip addresses
    if (ipAddressRegEx.matcher(hostName).matches()) {
      return hostName;
    }

    if (hostName.endsWith(".")) {
      hostName = hostName.substring(0, hostName.length() - 1);
    }
    if (hostName.startsWith("*") && hostName.length() > 1) {
      hostName = hostName.substring(1);
    }
    if (hostName.length() != 0) {
      if (!invalidDomainCharactersRegEx.matcher(hostName).find()) {
        String parts[] = hostName.split("\\.");
        if (parts.length >= 2) {
          Collection<String> secondaryNames = TLDNamesCollection
              .getSecondaryNames(parts[parts.length - 1]);

          if (secondaryNames.size() != 0) {
            // see if second to last part matches secondary names for this TLD
            // or there is a wildcard expression for secondary name in rule set
            if (secondaryNames.contains(parts[parts.length - 2])
                || secondaryNames.contains("*")) {
              // ok secondary part is potentianlly part of secondary name ...

              // check to see the part in not explicitly excluded ...
              if (secondaryNames.contains("!" + parts[parts.length - 2])) {
                // in this case, this is an explicit override. second to last
                // part is NOT part of secondary name
                return buildRootNameString(hostName, parts, parts.length - 2);
              } else {
                // otherwise, we need at least three parts
                if (parts.length >= 3) {
                  return buildRootNameString(hostName, parts, parts.length - 3);
                }
              }
            }
            // ok second to last part does not match set of known secondary
            // names
            else {
              // make a wildcard string matching secondary name
              String extendedWildcard = "*." + parts[parts.length - 2];
              // if match, then this implies secondary name has two components
              if (secondaryNames.contains(extendedWildcard)) {

                if (parts.length >= 3) {
                  // this implies that there must be four parts to the name to
                  // extract root
                  // unless exlusion rule applies
                  String exclusionRule2 = "!" + parts[parts.length - 3] + "."
                      + parts[parts.length - 2];

                  // if exclusion rule is present ...
                  if (secondaryNames.contains(exclusionRule2)) {
                    // third part is NOT part of secondary name
                    return buildRootNameString(hostName, parts,
                        parts.length - 3);
                  } else {
                    // ok extended wildcard matched. we need 4 parts minimum
                    if (parts.length >= 4) {
                      return buildRootNameString(hostName, parts,
                          parts.length - 4);
                    }
                  }
                }
              }
              // at this point ... if the null name exists ...
              else if (secondaryNames.contains("")) {
                // return second part as root name
                return buildRootNameString(hostName, parts, parts.length - 2);
              }
            }
          }
        }
      }
    }
    return null;
  }

  /** The maximum length of a Name */
  private static final int MAXNAME                      = 255;

  /** The maximum length of a label a Name */
  private static final int MAXLABEL                     = 63;

  /** The maximum number of labels in a Name */
  private static final int MAXLABELS                    = 128;

  static Pattern           invalidDomainCharactersRegEx = Pattern
                                                            .compile("[^0-9a-z\\-\\._]");
  static Pattern           ipAddressRegEx               = Pattern
                                                            .compile("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$");
  static Pattern           numericOnly                  = Pattern
                                                            .compile("[0-9]*$");

  /**
   * validate a domain name
   * 
   * @param domainName
   * @return true if domain name is valid
   */
  public static boolean isValidDomainName(String domainName) {

    // check for invalid length (max 255 characters)
    if (domainName.length() > MAXNAME) {
      return false;
    }

    String candidate = domainName.toLowerCase();

    // check to see if this is an ip address
    if (ipAddressRegEx.matcher(candidate).matches()) {
      return true;
    }

    // check for invalid characters
    if (invalidDomainCharactersRegEx.matcher(candidate).matches()) {
      return false;
    }
    // split into parts
    String[] parts = domainName.split("\\.");

    // check for max labels constraint
    if (parts.length > MAXLABELS) {
      return false;
    }
    return extractRootDomainName(candidate) != null;
  }

  /**
   * calculate a url fingerprint for the passed in url string
   * 
   * @param urlString
   * @return URLFPV2 data structure representing canonical fingerprint for url
   *         OR null if the url is invalid
   */
  public static URLFPV2 getURLFPV2FromURL(String urlString) {

    try {
      // canonicalize the incoming url ...
      String canonicalURL = URLUtils.canonicalizeURL(urlString, true);

      if (canonicalURL != null) {
        return getURLFPV2FromCanonicalURL(canonicalURL);
      }
    } catch (MalformedURLException e) {
    }
    return null;
  }

  /**
   * calculate a url fingerprint given a GoogleURL object
   * 
   * @param urlObject
   * @return URLFPV2 data structure representing canonical fingerprint for url
   *         OR null if the url is invalid
   */
  public static URLFPV2 getURLFPV2FromURLObject(GoogleURL urlObject) {
    try {
      // canonicalize the incoming url ...
      String canonicalURL = URLUtils.canonicalizeURL(urlObject, true);

      if (canonicalURL != null) {
        return getURLFPV2FromCanonicalURL(canonicalURL);
      }
    } catch (MalformedURLException e) {
    }
    return null;
  }

  /**
   * calculate a url fingerprint given a <<PREVIOUSLY CANONICALIZED>> url.
   * 
   * @param canonicalURL
   * @return URLFPV2 data structure representing canonical fingerprint for url
   *         OR null if the url is invalid
   */
  public static URLFPV2 getURLFPV2FromCanonicalURL(String canonicalURL) {

    // create a url fp record
    URLFPV2 urlFP = new URLFPV2();

    urlFP.setUrlHash(URLFingerprint.generate64BitURLFPrint(canonicalURL));

    String hostName = fastGetHostFromURL(canonicalURL);
    String rootDomainName = null;

    if (hostName != null)
      rootDomainName = URLUtils.extractRootDomainName(hostName);

    if (hostName != null && rootDomainName != null) {
      // ok we want to strip the leading www. if necessary
      if (hostName.startsWith("www.")) {
        // ok now. one nasty hack ... :-(
        // if root name does not equal full host name ...
        if (!rootDomainName.equals(hostName)) {
          // strip the www. prefix
          hostName = hostName.substring(4);
        }
      }
      urlFP.setDomainHash(FPGenerator.std64.fp(hostName));
      urlFP.setRootDomainHash(FPGenerator.std64.fp(rootDomainName));
      return urlFP;
    }
    return null;
  }

  /** session id normalizer **/
  private static SessionIDURLNormalizer _sessionIdNormalizer = new SessionIDURLNormalizer();

  /**
   * canonicalize a given url. Use the GoogleURL canonicalization library to
   * canonicalize the url, then apply the session id normalization filter to
   * remove common session id patterns.
   * 
   * 
   * @param incomingURL
   * @param stripLeadingWWW
   *          - set to true to string www. prefix from the domain if present
   * @return a canonical representation of the passed in URL that can be safely
   *         used as a replacement for the original url
   * @throws MalformedURLException
   */

  public static String canonicalizeURL(String incomingURL,
      boolean stripLeadingWWW) throws MalformedURLException {

    GoogleURL urlObject = new GoogleURL(incomingURL);

    if (!urlObject.isValid()) {
      throw new MalformedURLException("URL:" + incomingURL + " is invalid");
    }

    return canonicalizeURL(urlObject, stripLeadingWWW);
  }

  /**
   * Canonicalize the given GoogleURL object.
   * 
   * @param urlObject
   * @param stripLeadingWWW
   * @return
   * @throws MalformedURLException
   */
  public static String canonicalizeURL(GoogleURL urlObject,
      boolean stripLeadingWWW) throws MalformedURLException {

    StringBuilder urlOut = new StringBuilder();

    urlOut.append(urlObject.getScheme());
    urlOut.append("://");

    if (urlObject.getUserName() != GoogleURL.emptyString) {
      urlOut.append(urlObject.getUserName());
      if (urlObject.getPassword() != GoogleURL.emptyString) {
        urlOut.append(":");
        urlOut.append(urlObject.getPassword());
      }
      urlOut.append("@");
    }

    String host = urlObject.getHost();
    if (host.endsWith(".")) {
      host = host.substring(0, host.length() - 1);
    }

    if (stripLeadingWWW) {
      if (host.startsWith("www.")) {
        // ok now. one nasty hack ... :-(
        // if root name is null or root name does not equal full host name ...
        String rootName = extractRootDomainName(host);
        if (rootName == null || !rootName.equals(host)) {
          // striping the www. prefix
          host = host.substring(4);
        }
      }
    }
    urlOut.append(host);

    if (urlObject.getPort() != GoogleURL.emptyString
        && !urlObject.getPort().equals("80")) {
      urlOut.append(":");
      urlOut.append(urlObject.getPort());
    }
    if (urlObject.getPath() != GoogleURL.emptyString) {
      int indexOfSemiColon = urlObject.getPath().indexOf(';');
      if (indexOfSemiColon != -1) {
        urlOut.append(urlObject.getPath().substring(0, indexOfSemiColon));
      } else {
        urlOut.append(urlObject.getPath());
      }
    }
    if (urlObject.getQuery() != GoogleURL.emptyString) {
      urlOut.append("?");
      urlOut.append(urlObject.getQuery());
    }

    String canonicalizedURL = urlOut.toString();

    // phase 2 - remove common session id patterns
    canonicalizedURL = _sessionIdNormalizer.normalize(canonicalizedURL, "");

    return canonicalizedURL;
  }

  private static String fastGetHostFromURL(String urlString) {

    int hostStart = urlString.indexOf(":");
    if (hostStart != -1) {

      hostStart++;

      int urlLength = urlString.length();

      while (hostStart < urlString.length()) {
        char nextChar = urlString.charAt(hostStart);
        if (nextChar != '/' && nextChar != '\\' && nextChar != '\n'
            && nextChar != '\r' && nextChar != '\t' && nextChar != ' ') {
          break;
        }
        hostStart++;
      }

      if (hostStart < urlLength) {

        int hostEnd = hostStart + 1;

        while (hostEnd < urlLength) {
          char nextChar = urlString.charAt(hostEnd);
          if (nextChar == '/' || nextChar == '?' || nextChar == ';'
              || nextChar == '#')
            break;
          hostEnd++;
        }

        int indexOfAt = urlString.indexOf("@", hostStart);
        if (indexOfAt != -1 && indexOfAt < hostEnd) {
          hostStart = indexOfAt + 1;
        }

        String host = urlString.substring(hostStart, hostEnd);

        int hostLength = host.length();
        int colonEnd = host.indexOf(":");
        if (colonEnd != -1) {
          hostLength = colonEnd;
          host = urlString.substring(hostStart, hostStart + hostLength);
        }

        GoogleURL urlObject = new GoogleURL("http://" + host);

        if (urlObject.isValid()) {
          return urlObject.getHost();
        }
      }
    }
    return null;
  }

}
