package org.commoncrawl.rpc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * An empty data structure
 * 
 * @author rana
 *
 */
public class EmptyStruct extends RPCStruct {

  static EmptyStruct singleton = new EmptyStruct();

  public static EmptyStruct getSingleton() {
    return singleton;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {

    throw new CloneNotSupportedException();
  }

  @Override
  public void deserialize(DataInput in, BinaryProtocol decoder)
      throws IOException {
    // NOOP
  }

  @Override
  public boolean equals(Object peer) {
    return false;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public void merge(Object peer) throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

  @Override
  public void serialize(DataOutput out, BinaryProtocol encoder)
      throws IOException {
    // noop
  }
}
