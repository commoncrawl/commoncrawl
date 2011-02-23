package org.commoncrawl.util.shared;

import java.net.URL;

import org.commoncrawl.util.shared.FPGenerator;

public class URLFingerprint {

  public static long generate64BitURLFPrint(URL theURL) {
    return FPGenerator.std64.fp(theURL.toString());
  }

  public static long generate64BitURLFPrint(String theURL) {
    return FPGenerator.std64.fp(theURL);
  }

  public static int generate32BitHostFP(String hostname) {
    return hostname.hashCode();
  }
}
