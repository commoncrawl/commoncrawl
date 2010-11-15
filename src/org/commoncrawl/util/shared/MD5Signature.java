package org.commoncrawl.util.shared;

import org.apache.hadoop.io.MD5Hash;

public class MD5Signature {

  public static FlexBuffer calculate(FlexBuffer content) {
    FlexBuffer signature = new FlexBuffer();

    if (content.getCount() != 0) {
      signature.set(MD5Hash.digest(content.get(), content.getOffset(),
          content.getCount()).getDigest());
    }
    return signature;
  }
}
