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
    public FlexBuffer bits = new FlexBuffer(); // the actual bits of the control stream

    public BitStream() {
      nbits = 0; // number of bits in the control stream
      bits.setCapacity(4); // the actual bits of the control stream
    }

    public BitStream(byte[] bits, int nbits) {
      this.nbits = nbits;
      this.bits.set(bits,false);
    }

    public BitStream(byte[] bits,int offset,int length, int nbits) {
      this.nbits = nbits;
      this.bits.set(bits,offset,length,false);
    }

    public BitStream(FlexBuffer buffer,int nbits){
      this.nbits = nbits;
      this.bits = buffer;
    }
    
    public void reset(FlexBuffer buffer,int nbits) { 
      this.nbits = nbits;
      this.bits  = buffer;
    }

    // add a bit to the encoding
    public BitStream addbit(int b) {
      int len = bits.getCapacity();
      // need to grow the bit list
      if (nbits == len * 8) {
        int newlen = (int) (len * 1.5) + 1;
        byte tmp[] = new byte[newlen];
        System.arraycopy(bits.get(),bits.getOffset(), tmp, 0, bits.getCapacity());
        bits.set(tmp,false);
      }
      if (b == 1)
        bits.get()[bits.getOffset() + (nbits >> 3)] |= (1 << (nbits & 0x7));
      nbits++;
      
      return this;
    }
    
    public int getBit(int index) {
      return (bits.get()[bits.getOffset() + (index >> 3)] >> (index & 0x7)) & 0x1;
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
      int bit = (_bitStream.bits.get()[_bitStream.bits.getOffset() + (_currPos >> 3)] >> (_currPos & 0x7)) & 0x1;
      _currPos++;
      return bit;
    }
  }
}
