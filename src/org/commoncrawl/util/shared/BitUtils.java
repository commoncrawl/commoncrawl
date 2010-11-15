package org.commoncrawl.util.shared;

/**
 * Helpers to write bit level data
 * 
 * @author rana
 * 
 */
public class BitUtils {

  /**
   * a writeable bitstream
   * 
   * @author rana
   * 
   */
  public static class BitStream {

    public int    nbits; // number of bits in the control stream
    public byte[] bits; // the actual bits of the control stream

    public BitStream() {
      nbits = 0; // number of bits in the control stream
      bits = new byte[4]; // the actual bits of the control stream
    }

    public BitStream(byte[] bits, int nbits) {
      this.nbits = nbits;
      this.bits = bits;
    }

    // add a bit to the encoding
    public void addbit(int b) {

      int len = bits.length;
      // need to grow the bit list
      if (nbits == len * 8) {
        int newlen = (int) (len * 1.5) + 1;
        byte tmp[] = new byte[newlen];
        System.arraycopy(bits, 0, tmp, 0, bits.length);
        bits = tmp;
      }
      if (b == 1)
        bits[(nbits >> 3)] |= (1 << (nbits & 0x7));
      nbits++;
    }
  }

  /**
   * a bitstream reader
   * 
   * @author rana
   * 
   */
  public static class BitStreamReader {

    BitStream _bitStream;
    int       _currPos;

    public BitStreamReader(BitStream bitStream) {
      _bitStream = bitStream;
      _currPos = 0;
    }

    public boolean hasNext() {
      return _currPos < _bitStream.nbits;
    }

    // get the value of the next bit in the stream
    public int getbit() {
      int bit = (_bitStream.bits[(_currPos >> 3)] >> (_currPos & 0x7)) & 0x1;
      _currPos++;
      return bit;
    }
  }

}
