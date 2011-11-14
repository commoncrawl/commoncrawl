package org.commoncrawl.util.shared;

import java.util.Collection;
import java.util.regex.Pattern;

/**
 * Utility class for validating domain names and extracting top level domain 
 * names.
 * 
 * @author rana
 *
 */
public class DomainNameUtils {

  static Pattern           invalidDomainCharactersRegEx = Pattern
                                                            .compile("[^0-9a-z\\-\\._]");
  static Pattern           ipAddressRegEx               = Pattern
                                                            .compile("^[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+$");
  static Pattern           numericOnly                  = Pattern
                                                            .compile("[0-9]*$");

  /** The maximum length of a Name */
  private static final int MAXNAME                      = 255;

  /** The maximum length of a label a Name */
  private static final int MAXLABEL                     = 63;

  /** The maximum number of labels in a Name */
  private static final int MAXLABELS                    = 128;

  public static boolean isValidDomainName(String name) {

    // check for invalid length (max 255 characters)
    if (name.length() > MAXNAME) {
      return false;
    }

    String candidate = name.toLowerCase();

    // check to see if this is an ip address
    if (ipAddressRegEx.matcher(candidate).matches()) {
      return true;
    }

    // check for invalid characters
    if (invalidDomainCharactersRegEx.matcher(candidate).matches()) {
      return false;
    }
    // split into parts
    String[] parts = name.split("\\.");

    // check for max labels constraint
    if (parts.length > MAXLABELS) {
      return false;
    }
    return extractRootDomainName(candidate) != null;
  }

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

  public static String extractRootDomainName(String candidate) {

    // special case for ip addresses
    if (ipAddressRegEx.matcher(candidate).matches()) {
      return candidate;
    }

    if (candidate.endsWith(".")) {
      candidate = candidate.substring(0, candidate.length() - 1);
    }
    if (candidate.startsWith("*") && candidate.length() > 1) {
      candidate = candidate.substring(1);
    }
    if (candidate.length() != 0) {
      if (!invalidDomainCharactersRegEx.matcher(candidate).find()) {
        String parts[] = candidate.split("\\.");
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
                return buildRootNameString(candidate, parts, parts.length - 2);
              } else {
                // otherwise, we need at least three parts
                if (parts.length >= 3) {
                  return buildRootNameString(candidate, parts, parts.length - 3);
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
                    return buildRootNameString(candidate, parts,
                        parts.length - 3);
                  } else {
                    // ok extended wildcard matched. we need 4 parts minimum
                    if (parts.length >= 4) {
                      return buildRootNameString(candidate, parts,
                          parts.length - 4);
                    }
                  }
                }
              }
              // at this point ... if the null name exists ...
              else if (secondaryNames.contains("")) {
                // return second part as root name
                return buildRootNameString(candidate, parts, parts.length - 2);
              }
            }
          }
        }
      }
    }
    return null;
  }

}
