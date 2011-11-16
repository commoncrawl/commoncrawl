package org.commoncrawl.util.shared;

/*       
 * fastutil: Fast & compact type-specific collections for Java
 *
 * Copyright (C) 2002-2007 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A hash set with with a fast, small-footprint implementation whose
 * {@linkplain it.unimi.dsi.fastutil.Hash.Strategy hashing strategy} is specified
 * at creation time.
 * 
 * <P>
 * Instances of this class use a hash table to represent a set. The table is
 * enlarged as needed when new entries are created, but it is <em>never</em>
 * made smaller (even on a {@link #clear()}). A family of {@linkplain #trim()
 * trimming methods} lets you control the size of the table; this is particularly
 * useful if you reuse instances of this class.
 * 
 * <P>
 * The enlargement speed is controlled by the <em>growth factor</em>, a positive
 * number. If the growth factor is <var>p</var>, then the table is enlarged each
 * time roughly by a factor 2<sup>p/16</sup>. By default, <var>p</var> is
 * {@link Hash#DEFAULT_GROWTH_FACTOR}, which means that the table is doubled at
 * each enlargement, but one can easily set more or less aggressive policies by
 * calling {@link #growthFactor(int)} (note that the growth factor is
 * <em>not</em> serialized: deserialized tables gets the
 * {@linkplain Hash#DEFAULT_GROWTH_FACTOR default growth factor}).
 * 
 * 
 * @see Hash
 * @see HashCommon
 */
public class LongOpenHashSet implements java.io.Serializable, Cloneable,Iterable<Long> {
  /** The array of keys. */
  protected transient long     key[];
  /** The array of occupancy states. */
  protected transient byte     state[];
  /** The acceptable load factor. */
  protected final float        f;
  /** Index into the prime list, giving the current table size. */
  protected transient int      p;
  /**
   * Threshold after which we rehash. It must be the table size times {@link #f}
   * .
   */
  protected transient int      maxFill;
  /**
   * Number of free entries in the table (may be less than the table size -
   * {@link #count} because of deleted entries).
   */
  protected transient int      free;
  /** Number of entries in the set. */
  protected int                count;

  /** The initial default size of a hash table. */
  public final static int      DEFAULT_INITIAL_SIZE  = 16;
  /** The default load factor of a hash table. */
  public final static float    DEFAULT_LOAD_FACTOR   = .75f;
  /**
   * The load factor for a (usually small) table that is meant to be
   * particularly fast.
   */
  public final static float    FAST_LOAD_FACTOR      = .5f;
  /**
   * The load factor for a (usually very small) table that is meant to be
   * extremely fast.
   */
  public final static float    VERY_FAST_LOAD_FACTOR = .25f;
  /** The default growth factor of a hash table. */
  public final static int      DEFAULT_GROWTH_FACTOR = 16;
  /** The state of a free hash table entry. */
  public final static byte     FREE                  = 0;
  /** The state of a occupied hash table entry. */
  public final static byte     OCCUPIED              = -1;
  /** The state of a hash table entry freed by a deletion. */
  public final static byte     REMOVED               = 1;

  /**
   * A list of primes to be used as table sizes. The <var>i</var>-th element is
   * the largest prime <var>p</var> smaller than
   * 2<sup>(<var>i</var>+28)/16</sup> and such that <var>p</var>-2 is also prime
   * (or 1, for the first few entries).
   */

  public final static int      PRIMES[]              = { 3, 3, 3, 3, 3, 3, 3,
      3, 3, 3, 5, 5, 5, 5, 5, 5, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 13, 13, 13,
      13, 13, 13, 13, 13, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 19, 31,
      31, 31, 31, 31, 31, 31, 43, 43, 43, 43, 43, 43, 43, 43, 61, 61, 61, 61,
      61, 73, 73, 73, 73, 73, 73, 73, 103, 103, 109, 109, 109, 109, 109, 139,
      139, 151, 151, 151, 151, 181, 181, 193, 199, 199, 199, 229, 241, 241,
      241, 271, 283, 283, 313, 313, 313, 349, 349, 349, 349, 421, 433, 463,
      463, 463, 523, 523, 571, 601, 619, 661, 661, 661, 661, 661, 823, 859,
      883, 883, 883, 1021, 1063, 1093, 1153, 1153, 1231, 1321, 1321, 1429,
      1489, 1489, 1621, 1699, 1789, 1873, 1951, 2029, 2131, 2143, 2311, 2383,
      2383, 2593, 2731, 2803, 3001, 3121, 3259, 3391, 3583, 3673, 3919, 4093,
      4273, 4423, 4651, 4801, 5023, 5281, 5521, 5743, 5881, 6301, 6571, 6871,
      7129, 7489, 7759, 8089, 8539, 8863, 9283, 9721, 10141, 10531, 11071,
      11551, 12073, 12613, 13009, 13759, 14323, 14869, 15649, 16363, 17029,
      17839, 18541, 19471, 20233, 21193, 22159, 23059, 24181, 25171, 26263,
      27541, 28753, 30013, 31321, 32719, 34213, 35731, 37309, 38923, 40639,
      42463, 44281, 46309, 48313, 50461, 52711, 55051, 57529, 60091, 62299,
      65521, 68281, 71413, 74611, 77713, 81373, 84979, 88663, 92671, 96739,
      100801, 105529, 109849, 115021, 120079, 125509, 131011, 136861, 142873,
      149251, 155863, 162751, 169891, 177433, 185071, 193381, 202129, 211063,
      220021, 229981, 240349, 250969, 262111, 273643, 285841, 298411, 311713,
      325543, 339841, 355009, 370663, 386989, 404269, 422113, 440809, 460081,
      480463, 501829, 524221, 547399, 571603, 596929, 623353, 651019, 679909,
      709741, 741343, 774133, 808441, 844201, 881539, 920743, 961531, 1004119,
      1048573, 1094923, 1143283, 1193911, 1246963, 1302181, 1359733, 1420039,
      1482853, 1548541, 1616899, 1688413, 1763431, 1841293, 1922773, 2008081,
      2097133, 2189989, 2286883, 2388163, 2493853, 2604013, 2719669, 2840041,
      2965603, 3097123, 3234241, 3377191, 3526933, 3682363, 3845983, 4016041,
      4193803, 4379719, 4573873, 4776223, 4987891, 5208523, 5439223, 5680153,
      5931313, 6194191, 6468463, 6754879, 7053331, 7366069, 7692343, 8032639,
      8388451, 8759953, 9147661, 9552733, 9975193, 10417291, 10878619,
      11360203, 11863153, 12387841, 12936529, 13509343, 14107801, 14732413,
      15384673, 16065559, 16777141, 17519893, 18295633, 19105483, 19951231,
      20834689, 21757291, 22720591, 23726449, 24776953, 25873963, 27018853,
      28215619, 29464579, 30769093, 32131711, 33554011, 35039911, 36591211,
      38211163, 39903121, 41669479, 43514521, 45441199, 47452879, 49553941,
      51747991, 54039079, 56431513, 58930021, 61539091, 64263571, 67108669,
      70079959, 73182409, 76422793, 79806229, 83339383, 87029053, 90881083,
      94906249, 99108043, 103495879, 108077731, 112863013, 117860053,
      123078019, 128526943, 134217439, 140159911, 146365159, 152845393,
      159612601, 166679173, 174058849, 181765093, 189812341, 198216103,
      206991601, 216156043, 225726379, 235720159, 246156271, 257054491,
      268435009, 280319203, 292730833, 305691181, 319225021, 333358513,
      348117151, 363529759, 379624279, 396432481, 413983771, 432312511,
      451452613, 471440161, 492312523, 514109251, 536870839, 560640001,
      585461743, 611382451, 638450569, 666717199, 696235363, 727060069,
      759249643, 792864871, 827967631, 864625033, 902905501, 942880663,
      984625531, 1028218189, 1073741719, 1121280091, 1170923713, 1222764841,
      1276901371, 1333434301, 1392470281, 1454120779, 1518500173, 1585729993,
      1655935399, 1729249999, 1805811253, 1885761133, 1969251079, 2056437379,
      2147482951                                    };

  /**
   * The growth factor of the table. The next table size will be
   * <code>{@link Hash#PRIMES}[{@link #p}+growthFactor</code>.
   */
  protected transient int      growthFactor          = DEFAULT_GROWTH_FACTOR;
  public static final long     serialVersionUID      = -7046029254386353129L;
  private static final boolean ASSERTS               = false;

  /**
   * A type-specific {@link Iterator}; provides an additional method to avoid
   * (un)boxing, and the possibility to skip elements.
   * 
   * @see Iterator
   */
  public static interface LongIterator extends Iterator<Long> {
    /**
     * Returns the next element as a primitive type.
     * 
     * @return the next element in the iteration.
     * @see Iterator#next()
     */
    long nextLong();

    /**
     * Skips the given number of elements.
     * 
     * <P>
     * The effect of this call is exactly the same as that of calling
     * {@link #next()} for <code>n</code> times (possibly stopping if
     * {@link #hasNext()} becomes false).
     * 
     * @param n
     *          the number of elements to skip.
     * @return the number of elements actually skipped.
     * @see Iterator#next()
     */
    int skip(int n);
  }

  /**
   * Delegates to <code>remove()</code>.
   * 
   * @param k
   *          the element to be removed.
   * @return true if the set was modified.
   */
  public boolean rem(long k) {
    return remove(k);
  }

  /** Delegates to the corresponding type-specific method. */
  public boolean remove(final Object o) {
    return remove(((((Long) (o)).longValue())));
  }

  /**
   * Creates a new hash set.
   * 
   * The actual table size is the least available prime greater than
   * <code>n</code>/<code>f</code>.
   * 
   * @param n
   *          the expected number of elements in the hash set.
   * @param f
   *          the load factor.
   * @see Hash#PRIMES
   */
  @SuppressWarnings("unchecked")
  public LongOpenHashSet(final int n, final float f) {
    if (f <= 0 || f > 1)
      throw new IllegalArgumentException(
          "Load factor must be greater than 0 and smaller than or equal to 1");
    if (n < 0)
      throw new IllegalArgumentException("Hash table size must be nonnegative");
    int l = Arrays.binarySearch(PRIMES, (int) (n / f) + 1);
    if (l < 0)
      l = -l - 1;
    free = PRIMES[p = l];
    this.f = f;
    this.maxFill = (int) (free * f);
    key = new long[free];
    state = new byte[free];
  }

  /**
   * Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load
   * factor.
   * 
   * @param n
   *          the expected number of elements in the hash set.
   */
  public LongOpenHashSet(final int n) {
    this(n, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Creates a new hash set with {@link Hash#DEFAULT_INITIAL_SIZE} elements and
   * {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
   */
  public LongOpenHashSet() {
    this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Creates a new hash set using elements provided by a type-specific iterator.
   * 
   * @param i
   *          a type-specific iterator whose elements will fill the set.
   * @param f
   *          the load factor.
   */
  public LongOpenHashSet(final LongIterator i, final float f) {
    this(DEFAULT_INITIAL_SIZE, f);
    while (i.hasNext())
      add(i.nextLong());
  }

  /**
   * Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor
   * using elements provided by a type-specific iterator.
   * 
   * @param i
   *          a type-specific iterator whose elements will fill the set.
   */
  public LongOpenHashSet(final LongIterator i) {
    this(i, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Ensures that a range given by an offset and a length fits an array of given
   * length.
   * 
   * <P>
   * This method may be used whenever an array range check is needed.
   * 
   * @param arrayLength
   *          an array length.
   * @param offset
   *          a start index for the fragment
   * @param length
   *          a length (the number of elements in the fragment).
   * @throws IllegalArgumentException
   *           if <code>length</code> is negative.
   * @throws ArrayIndexOutOfBoundsException
   *           if <code>offset</code> is negative or <code>offset</code>+
   *           <code>length</code> is greater than <code>arrayLength</code>.
   */
  public static void ensureOffsetLength(final int arrayLength,
      final int offset, final int length) {
    if (offset < 0)
      throw new ArrayIndexOutOfBoundsException("Offset (" + offset
          + ") is negative");
    if (length < 0)
      throw new IllegalArgumentException("Length (" + length + ") is negative");
    if (offset + length > arrayLength)
      throw new ArrayIndexOutOfBoundsException("Last index ("
          + (offset + length) + ") is greater than array length ("
          + arrayLength + ")");
  }

  /**
   * Ensures that a range given by an offset and a length fits an array.
   * 
   * <P>
   * This method may be used whenever an array range check is needed.
   * 
   * @param a
   *          an array.
   * @param offset
   *          a start index.
   * @param length
   *          a length (the number of elements in the range).
   * @throws IllegalArgumentException
   *           if <code>length</code> is negative.
   * @throws ArrayIndexOutOfBoundsException
   *           if <code>offset</code> is negative or <code>offset</code>+
   *           <code>length</code> is greater than the array length.
   */
  private static void ensureOffsetLengthOfLongArray(final long[] a,
      final int offset, final int length) {
    ensureOffsetLength(a.length, offset, length);
  }

  /**
   * Creates a new hash set and fills it with the elements of a given array.
   * 
   * @param a
   *          an array whose elements will be used to fill the set.
   * @param offset
   *          the first element to use.
   * @param length
   *          the number of elements to use.
   * @param f
   *          the load factor.
   */
  public LongOpenHashSet(final long[] a, final int offset, final int length,
      final float f) {
    this(length < 0 ? 0 : length, f);
    ensureOffsetLengthOfLongArray(a, offset, length);
    for (int i = 0; i < length; i++)
      add(a[offset + i]);
  }

  /**
   * Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor
   * and fills it with the elements of a given array.
   * 
   * @param a
   *          an array whose elements will be used to fill the set.
   * @param offset
   *          the first element to use.
   * @param length
   *          the number of elements to use.
   */
  public LongOpenHashSet(final long[] a, final int offset, final int length) {
    this(a, offset, length, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Creates a new hash set copying the elements of an array.
   * 
   * @param a
   *          an array to be copied into the new hash set.
   * @param f
   *          the load factor.
   */
  public LongOpenHashSet(final long[] a, final float f) {
    this(a, 0, a.length, f);
  }

  /**
   * Creates a new hash set with {@link Hash#DEFAULT_LOAD_FACTOR} as load factor
   * copying the elements of an array.
   * 
   * @param a
   *          an array to be copied into the new hash set.
   */
  public LongOpenHashSet(final long[] a) {
    this(a, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Sets the growth factor. Subsequent enlargements will increase the table
   * size roughly by a multiplicative factor of 2<sup>p/16</sup>.
   * 
   * @param growthFactor
   *          the new growth factor; it must be positive.
   */
  public void growthFactor(int growthFactor) {
    if (growthFactor <= 0)
      throw new IllegalArgumentException("Illegal growth factor "
          + growthFactor);
    this.growthFactor = growthFactor;
  }

  /**
   * Gets the growth factor.
   * 
   * @return the growth factor of this set.
   * @see #growthFactor(int)
   */
  public int growthFactor() {
    return growthFactor;
  }

  /*
   * The following methods implements some basic building blocks used by all
   * accessors. They are (and should be maintained) identical to those used in
   * HashMap.drv.
   */
  /**
   * Searches for a key, keeping track of a possible insertion point.
   * 
   * @param k
   *          the key.
   * @return the index of the correct insertion point, if the key is not found;
   *         otherwise, <var>-i</var>-1, where <var>i</var> is the index of the
   *         entry containing the key.
   */
  protected final int findInsertionPoint(final long k) {
    final long key[] = this.key;
    final byte state[] = this.state;
    final int n = key.length;
    // First of all, we make the key into a positive integer.
    final int k2i = longHash2IntHash(k) & 0x7FFFFFFF;
    // The primary hash, a.k.a. starting point.
    int h1 = k2i % n;
    if (state[h1] == OCCUPIED && !((k) == (key[h1]))) {
      // The secondary hash.
      final int h2 = (k2i % (n - 2)) + 1;
      do
        h1 = (h1 + h2) % n;
      while (state[h1] == OCCUPIED && !((k) == (key[h1]))); // There's always a
                                                            // FREE entry.
    }
    if (state[h1] == FREE)
      return h1;
    if (state[h1] == OCCUPIED)
      return -h1 - 1; // Necessarily, KEY_EQUALS_HASH( k, h, key[ h1 ] ).
    /* Tables without deletions will never use code beyond this point. */
    final int i = h1; // Remember first available bucket for later.
    /** See the comments in the documentation of the interface Hash. */
    if (ASSERTS)
      assert state[h1] == REMOVED;
    if (!((k) == (key[h1]))) {
      // The secondary hash.
      final int h2 = (k2i % (n - 2)) + 1;
      do
        h1 = (h1 + h2) % n;
      while (state[h1] != FREE && !((k) == (key[h1])));
    }
    return state[h1] == OCCUPIED ? -h1 - 1 : i; // In the first case,
                                                // necessarily, KEY_EQUALS_HASH(
                                                // k, h, key[ h1 ] ).
  }

  /**
   * Searches for a key.
   * 
   * @param k
   *          the key.
   * @return the index of the entry containing the key, or -1 if the key wasn't
   *         found.
   */
  protected final int findKey(final long k) {
    final long key[] = this.key;
    final byte state[] = this.state;
    final int n = key.length;
    // First of all, we make the key into a positive integer.
    final int k2i = longHash2IntHash(k) & 0x7FFFFFFF;
    // The primary hash, a.k.a. starting point.
    int h1 = k2i % n;
    /** See the comments in the documentation of the interface Hash. */
    if (state[h1] != FREE && !((k) == (key[h1]))) {
      // The secondary hash.
      final int h2 = (k2i % (n - 2)) + 1;
      do
        h1 = (h1 + h2) % n;
      while (state[h1] != FREE && !((k) == (key[h1]))); // There's always a FREE
                                                        // entry.
    }
    return state[h1] == OCCUPIED ? h1 : -1; // In the first case, necessarily,
                                            // KEY_EQUALS_HASH( k, h, key[ h1 ]
                                            // ).
  }

  public boolean add(final long k) {
    final int i = findInsertionPoint(k);
    if (i < 0)
      return false;
    if (state[i] == FREE)
      free--;
    state[i] = OCCUPIED;
    key[i] = k;
    if (++count >= maxFill) {
      int newP = Math.min(p + growthFactor, PRIMES.length - 1);
      // Just to be sure that size changes when p is very small.
      while (PRIMES[newP] == PRIMES[p])
        newP++;
      rehash(newP); // Table too filled, let's rehash
    }
    if (free == 0)
      rehash(p);
    if (ASSERTS)
      checkTable();
    return true;
  }

  @SuppressWarnings("unchecked")
  public boolean remove(final long k) {
    final int i = findKey(k);
    if (i < 0)
      return false;
    state[i] = REMOVED;
    count--;
    if (ASSERTS)
      checkTable();
    return true;
  }

  @SuppressWarnings("unchecked")
  public boolean contains(final long k) {
    return findKey(k) >= 0;
  }

  private static void fill(final byte[] array, final byte value) {
    int i = array.length;
    while (i-- != 0)
      array[i] = value;
  }

  /*
   * Removes all elements from this set.
   * 
   * <P>To increase object reuse, this method does not change the table size. If
   * you want to reduce the table size, you must use {@link #trim()}.
   */
  public void clear() {
    if (free == state.length)
      return;
    free = state.length;
    count = 0;
    fill(state, FREE);
  }

  /** An iterator over a hash set. */
  private class SetIterator implements LongIterator {
    /** The index of the next entry to be returned. */
    int pos  = 0;
    /** The index of the last entry that has been returned. */
    int last = -1;
    /** A downward counter measuring how many entries have been returned. */
    int c    = count;
    {
      final byte state[] = LongOpenHashSet.this.state;
      final int n = state.length;
      if (c != 0)
        while (pos < n && state[pos] != OCCUPIED)
          pos++;
    }

    /** Delegates to the corresponding type-specific method. */
    public Long next() {
      return Long.valueOf(nextLong());
    }

    /**
     * This method just iterates the type-specific version of {@link #next()}
     * for at most <code>n</code> times, stopping if {@link #hasNext()} becomes
     * false.
     */
    public int skip(final int n) {
      int i = n;
      while (i-- != 0 && hasNext())
        nextLong();
      return n - i - 1;
    }

    public boolean hasNext() {
      return c != 0 && pos < LongOpenHashSet.this.state.length;
    }

    public long nextLong() {
      long retVal;
      final byte state[] = LongOpenHashSet.this.state;
      final int n = state.length;
      if (!hasNext())
        throw new NoSuchElementException();
      retVal = key[last = pos];
      if (--c != 0)
        do
          pos++;
        while (pos < n && state[pos] != OCCUPIED);
      return retVal;
    }

    @SuppressWarnings("unchecked")
    public void remove() {
      if (last == -1 || state[last] != OCCUPIED)
        throw new IllegalStateException();
      state[last] = REMOVED;
      count--;
    }
  }

  public LongIterator iterator() {
    return new SetIterator();
  }

  /**
   * Rehashes this set without changing the table size.
   * 
   * <P>
   * This method should be called when the set underwent numerous deletions and
   * insertions. In this case, free entries become rare, and unsuccessful
   * searches require probing <em>all</em> entries. For reasonable load factors
   * this method is linear in the number of entries. You will need as much
   * additional free memory as that occupied by the table.
   * 
   * <P>
   * If you need to reduce the table siza to fit exactly this set, you must use
   * {@link #trim()}.
   * 
   * @return true if there was enough memory to rehash the set, false otherwise.
   * @see #trim()
   */
  public boolean rehash() {
    try {
      rehash(p);
    } catch (OutOfMemoryError cantDoIt) {
      return false;
    }
    return true;
  }

  /**
   * Rehashes this set, making the table as small as possible.
   * 
   * <P>
   * This method rehashes the table to the smallest size satisfying the load
   * factor. It can be used when the set will not be changed anymore, so to
   * optimize access speed (by collecting deleted entries) and size.
   * 
   * <P>
   * If the table size is already the minimum possible, this method does
   * nothing. If you want to guarantee rehashing, use {@link #rehash()}.
   * 
   * @return true if there was enough memory to trim the set.
   * @see #trim(int)
   * @see #rehash()
   */
  public boolean trim() {
    int l = Arrays.binarySearch(PRIMES, (int) (count / f) + 1);
    if (l < 0)
      l = -l - 1;
    if (l >= p)
      return true;
    try {
      rehash(l);
    } catch (OutOfMemoryError cantDoIt) {
      return false;
    }
    return true;
  }

  /**
   * Rehashes this set if the table is too large.
   * 
   * <P>
   * Let <var>N</var> be the smallest table size that can hold
   * <code>max(n,{@link #size()})</code> entries, still satisfying the load
   * factor. If the current table size is smaller than or equal to <var>N</var>,
   * this method does nothing. Otherwise, it rehashes this set in a table of
   * size <var>N</var>.
   * 
   * <P>
   * This method is useful when reusing sets. {@linkplain #clear() Clearing a
   * set} leaves the table size untouched. If you are reusing a set many times,
   * you can call this method with a typical size to avoid keeping around a very
   * large table just because of a few large transient sets.
   * 
   * @param n
   *          the threshold for the trimming.
   * @return true if there was enough memory to trim the set.
   * @see #trim()
   * @see #rehash()
   */
  public boolean trim(final int n) {
    int l = Arrays.binarySearch(PRIMES,
        (int) (Math.min(Integer.MAX_VALUE - 1, Math.max(n, count) / f)) + 1);
    if (l < 0)
      l = -l - 1;
    if (p <= l)
      return true;
    try {
      rehash(l);
    } catch (OutOfMemoryError cantDoIt) {
      return false;
    }
    return true;
  }

  /**
   * Resizes the set.
   * 
   * <P>
   * This method implements the basic rehashing strategy, and may be overriden
   * by subclasses implementing different rehashing strategies (e.g., disk-based
   * rehashing). However, you should not override this method unless you
   * understand the internal workings of this class.
   * 
   * @param newP
   *          the new size as an index in {@link Hash#PRIMES}.
   */
  @SuppressWarnings("unchecked")
  protected void rehash(final int newP) {
    int i = 0, j = count, k2i, h1, h2;
    final byte state[] = this.state;
    // System.err.println("Rehashing to size " + PRIMES[newP] + "...");
    long k;
    final int newN = PRIMES[newP];
    final long key[] = this.key, newKey[] = new long[newN];
    final byte newState[] = new byte[newN];
    while (j-- != 0) {
      while (state[i] != OCCUPIED)
        i++;
      k = key[i];
      k2i = longHash2IntHash(k) & 0x7FFFFFFF;
      h1 = k2i % newN;
      if (newState[h1] != FREE) {
        h2 = (k2i % (newN - 2)) + 1;
        do
          h1 = (h1 + h2) % newN;
        while (newState[h1] != FREE);
      }
      newState[h1] = OCCUPIED;
      newKey[h1] = k;
      i++;
    }
    p = newP;
    free = newN - count;
    maxFill = (int) (newN * f);
    this.key = newKey;
    this.state = newState;
  }

  public int size() {
    return count;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  /**
   * Returns a deep copy of this set.
   * 
   * <P>
   * This method performs a deep copy of this hash set; the data stored in the
   * set, however, is not cloned. Note that this makes a difference only for
   * object keys.
   * 
   * @return a deep copy of this set.
   */
  @SuppressWarnings("unchecked")
  public Object clone() {
    LongOpenHashSet c;
    try {
      c = (LongOpenHashSet) super.clone();
    } catch (CloneNotSupportedException cantHappen) {
      throw new InternalError();
    }
    c.key = key.clone();
    c.state = state.clone();
    return c;
  }

  /**
   * Returns a hash code for this set.
   * 
   * This method overrides the generic method provided by the superclass. Since
   * <code>equals()</code> is not overriden, it is important that the value
   * returned by this method is the same value as the one returned by the
   * overriden method.
   * 
   * @return a hash code for this set.
   */
  public int hashCode() {
    int h = 0, i = 0, j = count;
    while (j-- != 0) {
      while (state[i] != OCCUPIED)
        i++;
      h += longHash2IntHash(key[i]);
      i++;
    }
    return h;
  }

  private void writeObject(java.io.ObjectOutputStream s)
      throws java.io.IOException {
    final LongIterator i = iterator();
    int j = count;
    s.defaultWriteObject();
    while (j-- != 0)
      s.writeLong(i.nextLong());
  }

  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream s)
      throws java.io.IOException, ClassNotFoundException {
    s.defaultReadObject();
    // We restore the default growth factor.
    growthFactor = DEFAULT_GROWTH_FACTOR;
    // Note that we DO NOT USE the stored p. See CHANGES.
    p = Arrays.binarySearch(PRIMES, (int) (count / f) + 1);
    if (p < 0)
      p = -p - 1;
    final int n = PRIMES[p];
    maxFill = (int) (n * f);
    free = n - count;
    ;
    final long key[] = this.key = new long[n];
    final byte state[] = this.state = new byte[n];
    int i, k2i, h1, h2;
    long k;
    i = count;
    while (i-- != 0) {
      k = s.readLong();
      k2i = longHash2IntHash(k) & 0x7FFFFFFF;
      h1 = k2i % n;
      if (state[h1] != FREE) {
        h2 = (k2i % (n - 2)) + 1;
        do
          h1 = (h1 + h2) % n;
        while (state[h1] != FREE);
      }
      state[h1] = OCCUPIED;
      key[h1] = k;
    }
    if (ASSERTS)
      checkTable();
  }

  private void checkTable() {
  }

  /** Returns the hash code that would be returned by {@link Long#hashCode()}.
   *
   * @return the same code as {@link Long#hashCode() new Long(f).hashCode()}.
   */
  final public static int longHash2IntHash( final long l ) {
      return (int)( l ^ ( l >>> 32 ) );
  }

}
