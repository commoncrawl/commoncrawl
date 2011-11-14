package org.commoncrawl.util.shared;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * adds offset support to BinaryComparable
 * 
 * @author rana
 *
 */
public abstract class BinaryComparableWithOffset extends BinaryComparable implements WritableComparable<BinaryComparable> {

  /**
   * get the offset into the underlying byte array  
   * @return
   */
  public abstract int getOffset();
  
  /**
   * Compare bytes from {#getBytes()}.
   * @see org.apache.hadoop.io.WritableComparator#compareBytes(byte[],int,int,byte[],int,int)
   */
  public int compareTo(BinaryComparable other) {
    if (this == other)
      return 0;
    if (other instanceof BinaryComparableWithOffset) { 
      return WritableComparator.compareBytes(getBytes(), getOffset(), getLength(),
          other.getBytes(), ((BinaryComparableWithOffset)other).getOffset(), other.getLength());
    }
    else { 
      return WritableComparator.compareBytes(getBytes(), getOffset(), getLength(),
             other.getBytes(), 0, other.getLength());
    }
  }

  /**
   * Compare bytes from {#getBytes()} to those provided.
   */
  public int compareTo(byte[] other, int off, int len) {
    return WritableComparator.compareBytes(getBytes(), getOffset(), getLength(),
      other, off, len);
  }

}
