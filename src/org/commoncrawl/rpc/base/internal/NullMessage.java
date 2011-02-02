package org.commoncrawl.rpc.base.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.commoncrawl.rpc.base.shared.BinaryProtocol;
import org.commoncrawl.rpc.base.shared.RPCStruct;

public class NullMessage extends RPCStruct {

  static NullMessage singleton = new NullMessage();

  public static NullMessage getSingleton() {
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
