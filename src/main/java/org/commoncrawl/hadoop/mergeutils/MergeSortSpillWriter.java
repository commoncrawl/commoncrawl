package org.commoncrawl.hadoop.mergeutils;

/*
 *    Copyright 2010 - CommonCrawl Foundation
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Reporter;
import org.commoncrawl.hadoop.mergeutils.OptimizedKeyGeneratorAndComparator.OptimizedKey;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.FileUtils;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * 
 * The workhorse of the library. This class (currently) implements SpillWriter and thus can accept
 * a very large number of <<unsorted>> Key/Value pairs, which it then sorts in batches and stores in a 
 * temp directory, and finally merges everything back into one sorted output via a merge sort operation.
 * 
 * This class implements two constructors, one that takes a RawComparator and one that takes an 
 * OptimizedKeyGeneratorAndComparator instance. The former is used to compare Raw Key/Value pairs,
 * while the later is used to create a an optimized key representation which can then be used for 
 * sorting within the segment as well as during the final merge sort. 
 * 
 * NOTE: IGNORE THE COMBINER OPTION FOR NOW. IT NEEDS MORE VETTING.
 * 
 * @author rana
 *
 * @param <KeyType> 	WritableComparable Type you will be using to represent a key 
 * @param <ValueType> Writable Type you will be using to represent a value 
 */

public class MergeSortSpillWriter<KeyType extends WritableComparable,ValueType extends Writable> 
implements SpillWriter<KeyType,ValueType> {

	public static final Log LOG = LogFactory.getLog(MergeSortSpillWriter.class);


	private static int DEFAULT_SPILL_INDEX_BUFFER_SIZE =  1000000; // 100K * 4 = 40000K .. OR 40MB // memory usage
	private static int SPILL_DATA_ITEM_AVG_SIZE = 400;
	private static int DEFAULT_SPILL_DATA_BUFFER_SIZE  = DEFAULT_SPILL_INDEX_BUFFER_SIZE * SPILL_DATA_ITEM_AVG_SIZE; // 1 *10(6) * 4 * 10(2) = 10(8) = 400MB  // memory usage
	private static int DEFAULT_REPLICATION_FACTOR = 2;
	
	// the number of index items we can store in ram 
	public static final String SPILL_INDEX_BUFFER_SIZE_PARAM 	= "commoncrawl.spill.indexbuffer.size";
	// the size of the data buffer used to accumulate key value pairs during the sort / spill process
	public static final String SPILL_DATA_BUFFER_SIZE_PARAM 	= "commoncrawl.spill.databuffer.size";
	// replication factor to use for intermediate spill files
	public static final String SPILL_FILE_REPLICATION_FACTOR_PARAM    = "commoncrawl.spill.replication.factor";

	FileSystem    _tempDataFileSystem;
	Configuration _conf;

	int          _spillIndexBuffer[];
	ByteBuffer   _spillDataBuffer;
	byte[] 			 _spillDataBufferBytes;
	int          _spillItemCount = 0;
	private  NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();

	Vector<Path> _mergeSegements = new Vector<Path>();
	RawKeyValueComparator<KeyType,ValueType> _comparator = null;
	OptimizedKeyGeneratorAndComparator<KeyType, ValueType> _optimizedKeyGenerator = null;
	OptimizedKey _optimizedKey = null;
	SpillValueCombiner<KeyType,ValueType>          _optCombiner = null;


	RawDataSpillWriter<KeyType,ValueType> _outputSpillWriter = null;
	Path        	 _temporaryDirectoryPath;
	Class<KeyType> _keyClass;
	Class<ValueType> _valueClass;
	DataOutputStream _outputStream;
	DataInputStream  _inputStream;
	CompressionCodec _compressionCodec;
	Reporter _reporter;
	int _spillIndexBufferSize = -1;
	int _spillDataBufferSize  = -1;


	/**
	 * use this constructor for creating a merger that can use an optinal combiner 
	 * but one that does not user optimized generated keys 
	 * 
	 * @param conf
	 * @param outputSpillWriter
	 * @param tempDataFileSystem
	 * @param tempDirectoryPath
	 * @param comparator
	 * @param optCombiner
	 * @param keyClass
	 * @param valueClass
	 * @param compressionCodec
	 * @param reporter
	 * @throws IOException
	 */
	public MergeSortSpillWriter(
			Configuration conf,
			RawDataSpillWriter<KeyType,ValueType> outputSpillWriter,
			FileSystem tempDataFileSystem,
			Path tempDirectoryPath,
			SpillValueCombiner<KeyType,ValueType> optionalCombiner,
			RawKeyValueComparator<KeyType,ValueType> comparator,
			Class<KeyType> keyClass,
			Class<ValueType> valueClass,
			CompressionCodec compressionCodec,
			Reporter reporter
	) throws IOException {

		init(conf,outputSpillWriter,tempDataFileSystem,tempDirectoryPath,comparator,null,optionalCombiner,keyClass,valueClass,compressionCodec,reporter);
	}

	/**
	 * use this constructor for creating a merger that uses an optimized key generator  
	 * 
	 * 
	 * @param conf
	 * @param outputSpillWriter
	 * @param tempDataFileSystem
	 * @param tempDirectoryPath
	 * @param keyGenerator
	 * @param keyClass
	 * @param valueClass
	 * @param compressionCodec
	 * @param reporter
	 * @throws IOException
	 */
	public MergeSortSpillWriter(
			Configuration conf,
			RawDataSpillWriter<KeyType,ValueType> outputSpillWriter,
			FileSystem tempDataFileSystem,
			Path tempDirectoryPath,
			OptimizedKeyGeneratorAndComparator<KeyType, ValueType> keyGenerator,
			Class<KeyType> keyClass,
			Class<ValueType> valueClass,
			CompressionCodec compressionCodec,
			Reporter reporter
	) throws IOException {

		init(conf,outputSpillWriter,tempDataFileSystem,tempDirectoryPath,null,keyGenerator,null,keyClass,valueClass,compressionCodec,reporter);
	}


	private void init(
			Configuration conf,
			RawDataSpillWriter<KeyType,ValueType> outputSpillWriter,
			FileSystem tempDataFileSystem,
			Path tempDirectoryPath,
			RawKeyValueComparator<KeyType,ValueType> comparator,
			OptimizedKeyGeneratorAndComparator<KeyType, ValueType> optionalGenerator,
			SpillValueCombiner<KeyType,ValueType> optCombiner,
			Class<KeyType> keyClass,
			Class<ValueType> valueClass,
			CompressionCodec compressionCodec,
			Reporter reporter
	) throws IOException { 

		_tempDataFileSystem = tempDataFileSystem;
		_conf = conf;
		_comparator = comparator;
		_optimizedKeyGenerator = optionalGenerator;
		if (_optimizedKeyGenerator != null) { 
			_optimizedKey = new OptimizedKey(_optimizedKeyGenerator.getGeneratedKeyType());
		}
		_optCombiner = optCombiner;
		_outputSpillWriter = outputSpillWriter;
		_keyClass = keyClass;
		_valueClass = valueClass;
		_temporaryDirectoryPath = new Path(tempDirectoryPath,Long.toString(Thread.currentThread().getId()) + "-" + System.currentTimeMillis());
		_tempDataFileSystem.delete(_temporaryDirectoryPath,true);
		_tempDataFileSystem.mkdirs(_temporaryDirectoryPath);
		NUMBER_FORMAT.setMinimumIntegerDigits(5);
		NUMBER_FORMAT.setGroupingUsed(false);
		_compressionCodec = compressionCodec;
		_reporter = reporter;
		// ok look up buffer sizes 
		_spillIndexBufferSize = _conf.getInt(SPILL_INDEX_BUFFER_SIZE_PARAM, DEFAULT_SPILL_INDEX_BUFFER_SIZE);
		LOG.info("SpillIndexBufferSize:" + _spillIndexBufferSize);
		_spillDataBufferSize  = _conf.getInt(SPILL_DATA_BUFFER_SIZE_PARAM, DEFAULT_SPILL_DATA_BUFFER_SIZE);
		LOG.info("SpillDataBufferSize:" + _spillDataBufferSize);
		// now allocate memory ... 
		_spillIndexBuffer = new int[_spillIndexBufferSize];
		_spillDataBuffer  = ByteBuffer.allocate(_spillDataBufferSize);
		_spillDataBufferBytes = _spillDataBuffer.array();
		_spillDataBuffer.clear();
		// initialize streams ... 
		_outputStream = new DataOutputStream(newOutputStream(_spillDataBuffer));
		_inputStream  = new DataInputStream(newInputStream(_spillDataBuffer));
	}

	private void freeMemory() { 
		_spillDataBuffer = null;
		_spillIndexBuffer = null;
	}

	@Override
	public void close() throws IOException {

		LOG.info("Entering flushAndClose");
		if (_spillItemCount == 0 && _mergeSegements.size() == 0) {
			LOG.info("No Data to Merge. Exiting Prematurely");

			freeMemory();
			// nothing to do.. return to caller..
			return;
		}

		// first check to see if anything left to sort 
		if (_spillItemCount != 0) {

			LOG.info("Trailing Spill Data Items of Count:" + _spillItemCount);

			// if no other merge segments ... spill directly to outer spill writer ... 
			if (_mergeSegements.size() == 0) {
				LOG.info("Merge Segment Count is zero. Sorting and Spilling to output file directly");
				// go ahead and sort and then spill the buffered data 
				sortAndSpill(_outputSpillWriter);
				// free memory ... 
				freeMemory();
				// and return
				return;
			}
			// otherwise do the normal spill to temporary file ... 
			else {
				LOG.info("Merge Segment Count non-zero. Sorting and Spilling to temporary file");
				sortAndSpill(null);
			}

			_spillItemCount = 0;

		}

		if (_mergeSegements.size() != 0) { 
			// now check to see how many spill files we have ...  
			if (_mergeSegements.size() == 1) { 
				// this is an error, since the check above should have flushed 
				// directly to to output spill writer for the single segment case 
				throw new IOException("Improper merge segment count. Expected >1 got 1");
			}

			LOG.info("Merging " + _mergeSegements.size() + " Segments");


			// do the final merge sort of the resulting paths ... 
			SequenceFileMerger<KeyType,ValueType> merger = null;

			if (_optimizedKeyGenerator != null) { 
				merger = new SequenceFileMerger<KeyType,ValueType>(
						_tempDataFileSystem,
						_conf,
						_mergeSegements,
						_outputSpillWriter,
						_keyClass,
						_valueClass,
						_optimizedKeyGenerator
				); 
			}
			else { 
				merger = new SequenceFileMerger<KeyType,ValueType>(
						_tempDataFileSystem,
						_conf,
						_mergeSegements,
						_outputSpillWriter,
						_keyClass,
						_valueClass,
						_optCombiner,
						_comparator
				); 
			}

			try {
				long mergeStartTime = System.currentTimeMillis();
				LOG.info("Starting Final Merge");
				// do the final merge and spill 
				merger.mergeAndSpill(_reporter);
				long mergeEndTime = System.currentTimeMillis();

				LOG.info("Final Merge took:" + (mergeEndTime - mergeStartTime));
			}
			catch (IOException e) { 
				LOG.error(CCStringUtils.stringifyException(e));
				throw e;
			}
			finally { 
				try { 
					merger.close();
				}
				catch (IOException e) { 
					LOG.error(CCStringUtils.stringifyException(e));
					throw e;
				}
				finally { 
					LOG.info("Deleting temporary files");
					for (Path path : _mergeSegements) { 
						// and delete temp directory ... 
						FileUtils.recursivelyDeleteFile(new File(path.toString()));
					}

					_tempDataFileSystem.delete(_temporaryDirectoryPath,true);

					_mergeSegements.clear();
				}
			}

		}
	}

	private Path getNextSpillFilePath() { 
		return new Path(_temporaryDirectoryPath,"part-" + NUMBER_FORMAT.format(_mergeSegements.size())); 
	}


	/** sort the current set of buffered records and spill **/
	private void sortAndSpill(RawDataSpillWriter<KeyType, ValueType> optionalWriter) throws IOException {

		// get byte pointer 
		byte[] bufferAsBytes = _spillDataBuffer.array();

		long sortAndSpillTime = System.currentTimeMillis();
		long sortTimeStart = System.currentTimeMillis();
		// merge items in buffer 
		if (_optimizedKeyGenerator != null) {

			if (_optimizedKey.getKeyType() == OptimizedKey.KEY_TYPE_LONG) { 
				LOG.info("Sorting:" + _spillItemCount + " Items Using Optimized Long Comparator");
				sortUsingOptimizedLongComparator(_spillIndexBuffer, 0,_spillItemCount);
			}
			else if (_optimizedKey.getKeyType() == OptimizedKey.KEY_TYPE_BUFFER) {
				LOG.info("Sorting:" + _spillItemCount + " Items Using Optimized Buffer Comparator");
				sortUsingOptimizedBufferKeys(bufferAsBytes,_spillIndexBuffer,0,_spillItemCount);
			}
			else if (_optimizedKey.getKeyType() == OptimizedKey.KEY_TYPE_LONG_AND_BUFFER) {
				LOG.info("Sorting:" + _spillItemCount + " Items Using Optimized Long And Buffer Only Comparator");
				sortUsingOptimizedLongAndBufferKeys(bufferAsBytes,_spillIndexBuffer,0,_spillItemCount);
			}
			else { 
				throw new IOException("Unknown Optimized Key Type!");
			}
		}
		else {
			LOG.info("Sorting:" + _spillItemCount + " Items Using Raw Comparator");
			sortUsingRawComparator(bufferAsBytes,_spillIndexBuffer, 0, _spillItemCount);
		}

		LOG.info("Sort Took:" + (System.currentTimeMillis() - sortTimeStart));

		Path spillFilePath = getNextSpillFilePath();

		// figure out if we are writing to temporary spill writer or passed in spill writer ... 
		RawDataSpillWriter spillWriter = null;

		boolean directSpillOutput = false;

		if (optionalWriter != null) {
			LOG.info("Writing spill output directory to final spill writer");
			spillWriter = optionalWriter;
			directSpillOutput = true;
		}
		else {
			LOG.info("Writing spill output to temporary spill file:" + spillFilePath);
			spillWriter = new SequenceFileSpillWriter<KeyType,ValueType>(_tempDataFileSystem,_conf,spillFilePath,_keyClass,_valueClass,null,_compressionCodec,
			    (short)_conf.getInt(SPILL_FILE_REPLICATION_FACTOR_PARAM, DEFAULT_REPLICATION_FACTOR));
		}

		long spillTimeStart = System.currentTimeMillis();

		try { 

			// iterated sorted items ...
			int i=0;
			int dataOffset=0;
			int keyLen = 0;
			int keyPos = 0;
			int valueLen = 0;
			int valuePosition = 0;
			int optimizedBufferLen = 0;


			for (i=0;i<_spillItemCount;++i) { 
				try { 
					dataOffset = _spillIndexBuffer[i];
					_spillDataBuffer.position(dataOffset);


					// if optimized key ... we need to write optimized key value as well as regular key value ... 
					if (_optimizedKeyGenerator != null) {
						// init optimized key length 
						_optimizedKey.readHeader(_inputStream);
						optimizedBufferLen = _optimizedKey.getDataBufferSize();
					}
					else { 
						optimizedBufferLen = 0;
					}
					// now read in key length 
					keyLen = _spillDataBuffer.getInt();
					// mark key position 
					keyPos = _spillDataBuffer.position();
					// now skip past key length 
					_spillDataBuffer.position(keyPos + keyLen);
					// read value length 
					valueLen = _spillDataBuffer.getInt();
					// mark value position 
					valuePosition = _spillDataBuffer.position();
					// now skip past it (and optimized key data)... 
					_spillDataBuffer.position(valuePosition + valueLen + optimizedBufferLen);

					//LOG.info("Spilling Raw Record: startPos:" + dataOffset + " optKeyBufferSize:" + optimizedBufferLen + " keySize:" + keyLen + " keyDataPos:" + keyPos+ " valueSize:" + valueLen + " valuePos:" + valuePosition);


					// now write this out to the sequence file ...

					// if direct spill, we don't write lengths and optimized bits to stream ... 
					if (directSpillOutput || _optimizedKeyGenerator == null) { 
						spillWriter.spillRawRecord(
								bufferAsBytes,
								keyPos,
								keyLen, 
								bufferAsBytes,
								valuePosition,
								valueLen);
					}
					// otherwise ... in the optimized key case ... 
					else {
						spillWriter.spillRawRecord(
								bufferAsBytes,
								dataOffset ,
								keyLen + _optimizedKey.getHeaderSize() + 4, 
								bufferAsBytes,
								valuePosition,
								valueLen + optimizedBufferLen);
					}
					// increment progress... 
					if (i % 100000 == 0) { 
						LOG.info("Spilled " + i + " Records");
						if (_reporter != null) { 
							_reporter.progress();
						}
					}
				}
				catch (Exception e) {
					LOG.error("Error in Iteration. "+ 
							" DataOffset:" + dataOffset +
							" index:" + i + 
							" keyLen:" + keyLen +
							" keyPos:" + keyPos+
							" valueLen:" + valueLen+
							" valuePos:" + valuePosition+
							" spillDataBufferSize:" + _spillDataBuffer.capacity()+
							" spillDataPosition:" + _spillDataBuffer.position()+
							" spillDataRemaining:" + _spillDataBuffer.remaining());

					CCStringUtils.stringifyException(e);
					throw new IOException(e);
				}
			}

		}
		finally {
			if (spillWriter != optionalWriter) {
				//LOG.info("Closing Spill File");
				spillWriter.close();
			}
		}
		LOG.info("Spill Took:" + (System.currentTimeMillis() - spillTimeStart));

		if (spillWriter != optionalWriter) { 
			//LOG.info("Adding spill file to merge segment list");
			// add to spill file vector 
			_mergeSegements.add(spillFilePath);
		}

		LOG.info("Spill and Sort for:" + _spillItemCount + " Took:" + (System.currentTimeMillis() - sortAndSpillTime));
		// reset spill data buffer 
		_spillDataBuffer.position(0);
		// reset spill item count
		_spillItemCount = 0;
	}


	static final int getIntB(byte[] bb,int offset) {
		return (int)((((bb[offset + 0] & 0xff) << 24) |
				((bb[offset + 1] & 0xff) << 16) |
				((bb[offset + 2] & 0xff) <<  8) |
				((bb[offset + 3] & 0xff) <<  0)));
	}

	private static final int compareUsingRawComparator(RawKeyValueComparator comparator,byte[] keyValueData1,int offset1,byte[] keyValueData2,int offset2) throws IOException { 

		ByteBuffer buffer = ByteBuffer.wrap(keyValueData1);
		int testKey1Length = buffer.getInt();
		int key1Length    = getIntB(keyValueData1,offset1);
		int key1Offset = offset1 + 4;
		int value1Offset = key1Offset + key1Length + 4;

		int value1Length  = getIntB(keyValueData1,key1Offset + key1Length);


		int key2Length    = getIntB(keyValueData2,offset2);
		int key2Offset = offset2 + 4;
		int value2Offset = key2Offset + key2Length + 4;
		int value2Length  = getIntB(keyValueData2,key2Offset + key2Length);

		return comparator.compareRaw(keyValueData1, key1Offset, key1Length, keyValueData2, key2Offset, key2Length, keyValueData1, value1Offset, value1Length, keyValueData2, value2Offset, value2Length);
	}

	/**
	 * Returns the index of the median of the three indexed longs.
	 */
	private final int med3UsingOptimizedComparatorWithLongs(int x[], int a, int b, int c)throws IOException {

		long aValue = _spillDataBuffer.getLong(x[a]);
		long bValue = _spillDataBuffer.getLong(x[b]);
		long cValue = _spillDataBuffer.getLong(x[c]);

		return (aValue < bValue ?
				(bValue < cValue ? b : aValue < cValue ? c : a) :
					(bValue > cValue ? b : aValue > cValue ? c : a));
	}

	/**
	 * Returns the index of the median of the three elements using raw key comparator.
	 */
	private final int med3UsingRawComparator(byte[] spillData,int x[], int a, int b, int c)throws IOException {

		return (
				compareUsingRawComparator(_comparator,spillData,x[a], spillData, x[b]) < 0 ? // x[a] < x[b] ?
						((compareUsingRawComparator(_comparator,spillData,x[b], spillData, x[c]) < 0) ? b :
							(compareUsingRawComparator(_comparator,spillData,x[a], spillData, x[c]) < 0) ? c: a) : // (x[b] < x[c] ? b : x[a] < x[c] ? c : a) : 
								((compareUsingRawComparator(_comparator,spillData,x[b], spillData, x[c]) > 0) ? b :
									(compareUsingRawComparator(_comparator,spillData,x[a], spillData, x[c]) > 0) ? c : a)); // (x[b] > x[c] ? b : x[a] > x[c] ? c : a));

	}

	/**
	 * Returns the index of the median of the three elements using optimized long and buffer key comparator.
	 */
	private final int med3UsingOptimizedComparatorWithLongsAndBuffer(byte[] spillData,int x[], int a, int b, int c)throws IOException {

		return (
				compareUsingOptimizedRawLongAndBufferValues(spillData,x[a], x[b]) < 0 ? // x[a] < x[b] ?
						((compareUsingOptimizedRawLongAndBufferValues(spillData,x[b], x[c]) < 0) ? b :
							(compareUsingOptimizedRawLongAndBufferValues(spillData,x[a], x[c]) < 0) ? c: a) : // (x[b] < x[c] ? b : x[a] < x[c] ? c : a) : 
								((compareUsingOptimizedRawLongAndBufferValues(spillData,x[b], x[c]) > 0) ? b :
									(compareUsingOptimizedRawLongAndBufferValues(spillData,x[a], x[c]) > 0) ? c : a)); // (x[b] > x[c] ? b : x[a] > x[c] ? c : a));

	}

	/**
	 * Returns the index of the median of the three elements using optimized buffer key comparator.
	 */
	private final int med3UsingOptimizedComparatorWithBuffer(byte[] spillData,int x[], int a, int b, int c)throws IOException {

		return (
				compareUsingOptimizedRawBufferValues(spillData,x[a], x[b]) < 0 ? // x[a] < x[b] ?
						((compareUsingOptimizedRawBufferValues(spillData,x[b], x[c]) < 0) ? b :
							(compareUsingOptimizedRawBufferValues(spillData,x[a], x[c]) < 0) ? c: a) : // (x[b] < x[c] ? b : x[a] < x[c] ? c : a) : 
								((compareUsingOptimizedRawBufferValues(spillData,x[b], x[c]) > 0) ? b :
									(compareUsingOptimizedRawBufferValues(spillData,x[a], x[c]) > 0) ? c : a)); // (x[b] > x[c] ? b : x[a] > x[c] ? c : a));

	}


	/**
	 * Sorts the specified sub-array of longs into ascending order.
	 *
	 * borrowed from the Arrays implementaiton
	 * 
	 * The sorting algorithm is a tuned quicksort, adapted from Jon
	 * L. Bentley and M. Douglas McIlroy's "Engineering a Sort Function",
	 * Software-Practice and Experience, Vol. 23(11) P. 1249-1265 (November
	 * 1993).  This algorithm offers n*log(n) performance on many data sets
	 * that cause other quicksorts to degrade to quadratic performance.
	 * 
	 */
	private void sortUsingRawComparator(byte[] dataBytes,int x[], int off, int len) throws IOException {
		byte spillData[] = _spillDataBuffer.array();

		//  Insertion sort on smallest arrays
		if (len < 7) {
			for (int i=off; i<len+off; i++)
				// for (int j=i; j>off && x[j-1]>x[j]; j--)
				for (int j=i; j>off && compareUsingRawComparator(_comparator,spillData,x[j-1], spillData, x[j]) > 0; j--)
					swap(x, j, j-1);
			return;
		}

		//  figure out what a partition element
		int m = off + (len >> 1);       // for small arrays, take middle element
		if (len > 7) {
			int l = off;
			int n = off + len - 1;
			if (len > 40) {        // for big arrays, take pseudo-median of 9
				int s = len/8;
				l = med3UsingRawComparator(dataBytes,x, l,     l+s, l+2*s);
				m = med3UsingRawComparator(dataBytes,x, m-s,   m,   m+s);
				n = med3UsingRawComparator(dataBytes,x, n-2*s, n-s, n);
			}
			m = med3UsingRawComparator(dataBytes,x, l, m, n); // for mid-sized arrays, take median of 3
		}
		int vOffset = x[m];

		//  Establish Invariant: v* (<v)* (>v)* v*
		int a = off, b = a, c = off + len - 1, d = c;
		while(true) {
			// while (b <= c && x[b] <= v) {
			while (b <= c && compareUsingRawComparator(_comparator,spillData,x[b], spillData, vOffset) <=0) {
				// if (x[b] == v)
				if (compareUsingRawComparator(_comparator,spillData,x[b], spillData, vOffset) == 0)
					swap(x, a++, b);
				b++;
			}
			// while (c >= b && x[c] >= v) {
			while (c >= b && compareUsingRawComparator(_comparator,spillData,x[c], spillData, vOffset) >= 0) {
				// if (x[c] == v)
				if (compareUsingRawComparator(_comparator,spillData,x[c], spillData, vOffset) == 0)
					swap(x, c, d--);
				c--;
			}
			if (b > c)
				break;
			swap(x, b++, c--);
		}

		//  Swap partition elements back to middle
		int s, n = off + len;
		s = Math.min(a-off, b-a  );  vecswap(x, off, b-s, s);
		s = Math.min(d-c,   n-d-1);  vecswap(x, b,   n-s, s);

		//  Recursively sort non-partition-elements
		if ((s = b-a) > 1)
			sortUsingRawComparator(dataBytes,x, off, s);
		if ((s = d-c) > 1)
			sortUsingRawComparator(dataBytes,x, n-s, s);
	}

	/**
	 * Sorts the specified sub-array of longs into ascending order.
	 * 
	 * borrowed from the Arrays implementaiton
	 * 
	 * The sorting algorithm is a tuned quicksort, adapted from Jon
	 * L. Bentley and M. Douglas McIlroy's "Engineering a Sort Function",
	 * Software-Practice and Experience, Vol. 23(11) P. 1249-1265 (November
	 * 1993).  This algorithm offers n*log(n) performance on many data sets
	 * that cause other quicksorts to degrade to quadratic performance.
	 * 
	 */
	private void sortUsingOptimizedLongComparator(int x[], int off, int len) throws IOException {

		//  Insertion sort on smallest arrays
		if (len < 7) {
			for (int i=off; i<len+off; i++)
				// for (int j=i; j>off && x[j-1]>x[j]; j--)
				for (int j=i; j>off && _spillDataBuffer.getLong(x[j-1]) > _spillDataBuffer.getLong(x[j]); j--)
					swap(x, j, j-1);
			return;
		}

		//  Choose a partition element, v
		int m = off + (len >> 1);       // Small arrays, middle element
		if (len > 7) {
			int l = off;
			int n = off + len - 1;
			if (len > 40) {        // Big arrays, pseudomedian of 9
				int s = len/8;
				l = med3UsingOptimizedComparatorWithLongs(x, l,     l+s, l+2*s);
				m = med3UsingOptimizedComparatorWithLongs(x, m-s,   m,   m+s);
				n = med3UsingOptimizedComparatorWithLongs(x, n-2*s, n-s, n);
			}
			m = med3UsingOptimizedComparatorWithLongs(x, l, m, n); // Mid-size, med of 3
		}

		long v = _spillDataBuffer.getLong(x[m]);
		// LOG.info("Debug:x[" + m + "]=" + v);

		//  Establish Invariant: v* (<v)* (>v)* v*
		int a = off, b = a, c = off + len - 1, d = c;
		while(true) {
			// while (b <= c && x[b] <= v) {

			while (b <= c && _spillDataBuffer.getLong(x[b]) <= v) {
				//LOG.info("Debug:x[" + b + "]=" + _spillDataBuffer.getLong(x[b]));
				// if (x[b] == v)
				if (_spillDataBuffer.getLong(x[b]) == v)
					swap(x, a++, b);
				b++;
			}
			// while (c >= b && x[c] >= v) {
			while (c >= b && _spillDataBuffer.getLong(x[c]) >= v) {
				//LOG.info("Debug:x[" + c + "]=" + _spillDataBuffer.getLong(x[c]));
				// if (x[c] == v)
				if (_spillDataBuffer.getLong(x[c]) == v)
					swap(x, c, d--);
				c--;
			}
			if (b > c)
				break;
			swap(x, b++, c--);
		}

		//  Swap partition elements back to middle
		int s, n = off + len;
		s = Math.min(a-off, b-a  );  vecswap(x, off, b-s, s);
		s = Math.min(d-c,   n-d-1);  vecswap(x, b,   n-s, s);

		//  Recursively sort non-partition-elements
		if ((s = b-a) > 1)
			sortUsingOptimizedLongComparator(x, off, s);
		if ((s = d-c) > 1)
			sortUsingOptimizedLongComparator(x, n-s, s);
	}

	/**
	 * Sorts the specified sub-array of longs into ascending order.
	 * 
	 * borrowed from the Arrays implementaiton
	 * 
	 * The sorting algorithm is a tuned quicksort, adapted from Jon
	 * L. Bentley and M. Douglas McIlroy's "Engineering a Sort Function",
	 * Software-Practice and Experience, Vol. 23(11) P. 1249-1265 (November
	 * 1993).  This algorithm offers n*log(n) performance on many data sets
	 * that cause other quicksorts to degrade to quadratic performance.
	 * 
	 */

	DataInputBuffer _reader1 = new DataInputBuffer();
	DataInputBuffer _reader2 = new DataInputBuffer();


	private final int compareUsingOptimizedRawLongAndBufferValues(byte[] dataAsBytes,int lValueDataOffset,int rValueDataOffset)throws IOException {
		final long lValue = _spillDataBuffer.getLong(lValueDataOffset);
		final long rValue = _spillDataBuffer.getLong(rValueDataOffset);
		int buffer1Len = _spillDataBuffer.getInt(lValueDataOffset + 8);
		int buffer2Len = _spillDataBuffer.getInt(rValueDataOffset + 8);
		int buffer1Offset = _spillDataBuffer.getInt(lValueDataOffset + 12);
		int buffer2Offset= _spillDataBuffer.getInt(rValueDataOffset + 12);

		int result = (lValue  > rValue) ? 1 : (lValue  < rValue) 
				? -1 : 0;
		if (result == 0) { 
			return _optimizedKeyGenerator.compareOptimizedBufferKeys(dataAsBytes, lValueDataOffset + buffer1Offset, buffer1Len, dataAsBytes, rValueDataOffset + buffer2Offset, buffer2Len);
		}
		return result;
	}

	private final int compareUsingOptimizedRawBufferValues(byte[] dataAsBytes,int lValueDataOffset,int rValueDataOffset)throws IOException { 
		int buffer1Len = _spillDataBuffer.getInt(lValueDataOffset);
		int buffer2Len = _spillDataBuffer.getInt(rValueDataOffset);
		int buffer1Offset = _spillDataBuffer.getInt(lValueDataOffset + 4);
		int buffer2Offset= _spillDataBuffer.getInt(rValueDataOffset + 4);

		return _optimizedKeyGenerator.compareOptimizedBufferKeys(dataAsBytes, lValueDataOffset + buffer1Offset, buffer1Len, dataAsBytes, rValueDataOffset + buffer2Offset, buffer2Len);
	}

	/**
	 * sort optimized long and buffer keys
	 * @param dataBytes
	 * @param x
	 * @param off
	 * @param len
	 * @throws IOException
	 */
	private void sortUsingOptimizedLongAndBufferKeys(byte[] dataBytes,int x[], int off, int len) throws IOException {

		byte spillData[] = _spillDataBuffer.array();

		//  Insertion sort on smallest arrays
		if (len < 7) {
			for (int i=off; i<len+off; i++)
				// for (int j=i; j>off && x[j-1]>x[j]; j--)
				for (int j=i; j>off && compareUsingOptimizedRawLongAndBufferValues(spillData,x[j-1], x[j]) > 0; j--)
					swap(x, j, j-1);
			return;
		}

		//  figure out what a partition element
		int m = off + (len >> 1);       // for small arrays, take middle element
		if (len > 7) {
			int l = off;
			int n = off + len - 1;
			if (len > 40) {        // for big arrays, take pseudo-median of 9
				int s = len/8;
				l = med3UsingOptimizedComparatorWithLongsAndBuffer(dataBytes,x, l,     l+s, l+2*s);
				m = med3UsingOptimizedComparatorWithLongsAndBuffer(dataBytes,x, m-s,   m,   m+s);
				n = med3UsingOptimizedComparatorWithLongsAndBuffer(dataBytes,x, n-2*s, n-s, n);
			}
			m = med3UsingOptimizedComparatorWithLongsAndBuffer(dataBytes,x, l, m, n); // for mid-sized arrays, take median of 3
		}
		int vOffset = x[m];

		//  Establish Invariant: v* (<v)* (>v)* v*
		int a = off, b = a, c = off + len - 1, d = c;
		while(true) {
			// while (b <= c && x[b] <= v) {
			while (b <= c && compareUsingOptimizedRawLongAndBufferValues(spillData,x[b], vOffset) <=0) {
				// if (x[b] == v)
				if (compareUsingOptimizedRawLongAndBufferValues(spillData,x[b],vOffset) == 0)
					swap(x, a++, b);
				b++;
			}
			// while (c >= b && x[c] >= v) {
			while (c >= b && compareUsingOptimizedRawLongAndBufferValues(spillData,x[c],vOffset) >= 0) {
				// if (x[c] == v)
				if (compareUsingOptimizedRawLongAndBufferValues(spillData,x[c], vOffset) == 0)
					swap(x, c, d--);
				c--;
			}
			if (b > c)
				break;
			swap(x, b++, c--);
		}

		//  Swap partition elements back to middle
		int s, n = off + len;
		s = Math.min(a-off, b-a  );  vecswap(x, off, b-s, s);
		s = Math.min(d-c,   n-d-1);  vecswap(x, b,   n-s, s);

		//  Recursively sort non-partition-elements
		if ((s = b-a) > 1)
			sortUsingOptimizedLongAndBufferKeys(dataBytes,x, off, s);
		if ((s = d-c) > 1)
			sortUsingOptimizedLongAndBufferKeys(dataBytes,x, n-s, s);
	}


	/**
	 * sort optimized long and buffer keys
	 * @param dataBytes
	 * @param x
	 * @param off
	 * @param len
	 * @throws IOException
	 */
	private void sortUsingOptimizedBufferKeys(byte[] dataBytes,int x[], int off, int len) throws IOException {

		byte spillData[] = _spillDataBuffer.array();

		//  Insertion sort on smallest arrays
		if (len < 7) {
			for (int i=off; i<len+off; i++)
				// for (int j=i; j>off && x[j-1]>x[j]; j--)
				for (int j=i; j>off && compareUsingOptimizedRawBufferValues(spillData,x[j-1], x[j]) > 0; j--)
					swap(x, j, j-1);
			return;
		}

		//  figure out what a partition element
		int m = off + (len >> 1);       // for small arrays, take middle element
		if (len > 7) {
			int l = off;
			int n = off + len - 1;
			if (len > 40) {        // for big arrays, take pseudo-median of 9
				int s = len/8;
				l = med3UsingOptimizedComparatorWithBuffer(dataBytes,x, l,     l+s, l+2*s);
				m = med3UsingOptimizedComparatorWithBuffer(dataBytes,x, m-s,   m,   m+s);
				n = med3UsingOptimizedComparatorWithBuffer(dataBytes,x, n-2*s, n-s, n);
			}
			m = med3UsingOptimizedComparatorWithBuffer(dataBytes,x, l, m, n); // for mid-sized arrays, take median of 3
		}
		int vOffset = x[m];

		//  Establish Invariant: v* (<v)* (>v)* v*
		int a = off, b = a, c = off + len - 1, d = c;
		while(true) {
			// while (b <= c && x[b] <= v) {
			while (b <= c && compareUsingOptimizedRawBufferValues(spillData,x[b], vOffset) <=0) {
				// if (x[b] == v)
				if (compareUsingOptimizedRawBufferValues(spillData,x[b],vOffset) == 0)
					swap(x, a++, b);
				b++;
			}
			// while (c >= b && x[c] >= v) {
			while (c >= b && compareUsingOptimizedRawBufferValues(spillData,x[c],vOffset) >= 0) {
				// if (x[c] == v)
				if (compareUsingOptimizedRawBufferValues(spillData,x[c], vOffset) == 0)
					swap(x, c, d--);
				c--;
			}
			if (b > c)
				break;
			swap(x, b++, c--);
		}

		//  Swap partition elements back to middle
		int s, n = off + len;
		s = Math.min(a-off, b-a  );  vecswap(x, off, b-s, s);
		s = Math.min(d-c,   n-d-1);  vecswap(x, b,   n-s, s);

		//  Recursively sort non-partition-elements
		if ((s = b-a) > 1)
			sortUsingOptimizedBufferKeys(dataBytes,x, off, s);
		if ((s = d-c) > 1)
			sortUsingOptimizedBufferKeys(dataBytes,x, n-s, s);
	}


	/**
	 * Swaps x[a] with x[b].
	 */
	private static void swap(int x[], int a, int b) {
		//LOG.info("Swapping [" + a + "][" + _spillDataBuffer.getLong(x[a]) +"] and [" + b + "][" + _spillDataBuffer.getLong(x[b]) + "]");
		int t = x[a];
		x[a] = x[b];
		x[b] = t;
	}

	/**
	 * Swaps x[a .. (a+n-1)] with x[b .. (b+n-1)].
	 */
	private void vecswap(int x[], int a, int b, int n) {
		for (int i=0; i<n; i++, a++, b++)
			swap(x, a, b);
	}

	/**
	 * Spill a Raw Record (support for TFile)
	 */
	/*
  //DISABLE THIS CODE UNTIL WE CAN DEVELOP A UNIT TEST TO VALIDATE IT 
  public void spillRawRecord(DataInputStream keyStream,DataInputStream valueStream) throws IOException {
    // if index is full , trigger merge as well 
    if (_spillItemCount == _spillIndexBufferSize) {
      //LOG.info("Spill Item Count == " + SPILL_INDEX_BUFFER_SIZE + ". Flushing");
      sortAndSpill(null);
    }


    boolean done = false;

    // ok, writing from a stream is a little trickier, since we could overflow at any moment 
    // and (in the case of TFile) in some cases the input stream is not rewindable
  	boolean wroteKey = false;
  	int totalKeyBytesWritten = 0;
  	int keySizePos = 0;
  	boolean wroteValue = false;
  	int totalValueBytesWritten = 0;
  	int valueSizePos = 0;


    while (!done) {

      // mark buffer position ...
      int startPositon = _spillDataBuffer.position();
      //LOG.info("Buffer start position:" + startPositon);

    	boolean overflow = false;
      try {
      	int optimizedKeyBufferSize = 0;
        // if optimized comparator is available ...
        if (_optimizedKeyGenerator != null) {
        	// we don't have access to key, value bits yet, so leave space for header bytes 
        	_spillDataBuffer.position(startPositon + OptimizedKey.FIXED_HEADER_SIZE);
        }
        // save key size position
        keySizePos = _spillDataBuffer.position();
        // LOG.info("keySizePos:" + keySizePos);

        // skip past key length 
        _spillDataBuffer.position(keySizePos + 4);
      	// and skip past key bytes already written (via overflow condition).. 
      	_spillDataBuffer.position(_spillDataBuffer.position() + totalKeyBytesWritten);

        // now if we did not finish writing key ...  
        if (!wroteKey) {
	        // write key into remaining bytes ...
	        int bufferBytesAvailable = _spillDataBuffer.remaining();
	        int keyBytesWritten = keyStream.read(_spillDataBufferBytes, _spillDataBuffer.position(), bufferBytesAvailable);
	        totalKeyBytesWritten += keyBytesWritten;
	        if (bufferBytesAvailable == keyBytesWritten) { 
	        	// ok we reached an overflow condition !!!
	        	throw new BufferOverflowException();
	        }
	        else {
	        	// advance cursor 
	        	_spillDataBuffer.position(_spillDataBuffer.position() + keyBytesWritten);
	        	// ok we are done writing key ... 
	        	wroteKey = true;
	        }
        }
        // now save value size position 
        valueSizePos = _spillDataBuffer.position();
        // and calculate key size 
        int keySize = valueSizePos - keySizePos - 4;
        // reseek back 
        _spillDataBuffer.position(keySizePos);
        // write out real key size
        _spillDataBuffer.putInt(keySize);
        // skip to value size position  + 4
        _spillDataBuffer.position(valueSizePos + 4);
        // and skip past already written data (via overflow condition) 
        _spillDataBuffer.position(_spillDataBuffer.position() + totalValueBytesWritten);
        // now if we did not finish writing value ...  
        if (!wroteValue) {
	        // write key into remaining bytes ...
	        int bufferBytesAvailable = _spillDataBuffer.remaining();
	        int valueBytesWritten = valueStream.read(_spillDataBufferBytes, _spillDataBuffer.position(), bufferBytesAvailable);
	        totalValueBytesWritten += valueBytesWritten;
	        if (bufferBytesAvailable == valueBytesWritten) { 
	        	// ok we reached an overflow condition !!!
	        	throw new BufferOverflowException();
	        }
	        else {
	        	// advance cursor 
	        	_spillDataBuffer.position(_spillDataBuffer.position() + valueBytesWritten);
	        	// ok we are done writing value bytes ... 
	        	wroteValue = true;
	        }
        }
        // save end position 
        int endPosition = _spillDataBuffer.position();
        // calculate value size 
        int valueSize = endPosition - valueSizePos - 4;
        // reseek back to value size pos
        _spillDataBuffer.position(valueSizePos);
        // write value size
        _spillDataBuffer.putInt(valueSize);
        // ok now ... if optimized key generation is required ...
        if (_optimizedKeyGenerator != null) {
        	// seek back to start position 
        	_spillDataBuffer.position(startPositon);
        	// gen optimized key 
        	_optimizedKeyGenerator.generateOptimizedKeyForRawPair(
        			_spillDataBufferBytes,
        			keySizePos+4,
        			totalKeyBytesWritten,
        			_spillDataBufferBytes,
        			valueSizePos+4,
        			totalValueBytesWritten,
        			_optimizedKey);
          // write out optimized key header 
        	// and calc trailing key buffer size 
        	optimizedKeyBufferSize = _optimizedKey.writeHeaderToStream(_outputStream);
        }
        // seek forward to end position 
        _spillDataBuffer.position(endPosition);
        // and now if there is optional optimized key buffer data ... 
        // append it at end 
        if (optimizedKeyBufferSize != 0) { 
        	_optimizedKey.writeBufferToStream(_outputStream);
        }
        // store start position in index buffer 
        _spillIndexBuffer[_spillItemCount] = startPositon;
        // increment ... 
        _spillItemCount++;

        //LOG.info("startPos:" + startPositon + " optKeySize:" + optimizedKeySize + " keySizePos:" + keySizePos + " keySize:" + keySize + " valueSizePos:" + valueSizePos + " valueSize:" + valueSize);

        done = true;
      }
      // trap for buffer overflow specifically
      catch (IllegalArgumentException e) { 
      	//LOG.info("IllegalArgumentException - Buffer Overflow detected while writing to Spill Data Buffer. Flushing");
      	overflow = true;
      }
      catch (BufferOverflowException e) {
        //LOG.info("BufferOverflowException - Buffer Overflow detected while writing to Spill Data Buffer. Flushing");
        overflow = true;
      }
      if (overflow) { 
      	// if overflow happened with start position at zero... this is bad news.
      	// it means that key + value + optimized bits > spill data buffer size!
      	if (startPositon == 0) { 
      		throw new IOException("FATAL: KeySize:" + totalKeyBytesWritten + " + ValueSize:" + totalValueBytesWritten + " GT SpillBufferSize:" + _spillDataBuffer.capacity());
      	}
      	// otherwise .. 
      	// overflow in the stream case is a little tricky ... 
      	// first rewind buffer position ... 
        _spillDataBuffer.position(startPositon);
        // sort and spill whatever we did fit into the buffer ...  
        sortAndSpill(null);
        // now, we need to reconstitute the buffer back to previous state
        if (totalKeyBytesWritten != 0) { 
        	// calculate offset ... 
        	int newKeyBytesOffset = 4;
        	// if optimized .. 
        	if (_optimizedKeyGenerator != null) { 
        		newKeyBytesOffset += OptimizedKey.FIXED_HEADER_SIZE;
        	}
        	// copy bits 
        	System.arraycopy(_spillDataBufferBytes, keySizePos + 4,_spillDataBufferBytes, newKeyBytesOffset, totalKeyBytesWritten);

        	// and copy value bits potentially 
        	if (totalValueBytesWritten != 0) { 
        		int newValueBytesOffset = newKeyBytesOffset + totalKeyBytesWritten + 4;
          	// copy bits 
          	System.arraycopy(_spillDataBufferBytes, valueSizePos + 4,_spillDataBufferBytes, newValueBytesOffset, totalValueBytesWritten);
        	}
        }

      }
    }
  }
	 */

	/**
	 * Spill a Typed Record
	 */
	@Override
	public void spillRecord(KeyType key, ValueType value) throws IOException {

		// if index is full , trigger merge as well 
		if (_spillItemCount == _spillIndexBufferSize) {
			//LOG.info("Spill Item Count == " + SPILL_INDEX_BUFFER_SIZE + ". Flushing");
			sortAndSpill(null);
		}


		boolean done = false;

		while (!done) {

			// mark buffer position ...
			int startPositon = _spillDataBuffer.position();
			//LOG.info("Buffer start position:" + startPositon);

			boolean overflow = false;
			try {
				// if optimized comparator is available ...
				if (_optimizedKeyGenerator != null) {
					// gen optimized key 
					_optimizedKeyGenerator.generateOptimizedKeyForPair(key, value,_optimizedKey);
					// ok skip header size for now ... 
					_spillDataBuffer.position(_spillDataBuffer.position() + _optimizedKey.getHeaderSize());
				}
				// save key size position
				int keySizePos = _spillDataBuffer.position();
				// LOG.info("keySizePos:" + keySizePos);
				// skip past key length 
				_spillDataBuffer.position(keySizePos + 4);
				// next write key and value 
				key.write(_outputStream);
				// now save value size position 
				int valueSizePos = _spillDataBuffer.position();
				// and calculate key size 
				int keySize = valueSizePos - keySizePos - 4;
				// reseek back 
				_spillDataBuffer.position(keySizePos);
				// write out real key size
				_spillDataBuffer.putInt(keySize);
				// skip to value size position  + 4
				_spillDataBuffer.position(valueSizePos + 4);
				// write out actual value 
				value.write(_outputStream);
				// save end position 
				int endPosition = _spillDataBuffer.position();
				// calculate value size 
				int valueSize = endPosition - valueSizePos - 4;
				// reseek back to value size pos
				_spillDataBuffer.position(valueSizePos);
				// write value size
				_spillDataBuffer.putInt(valueSize);
				// seek forward to end position 
				_spillDataBuffer.position(endPosition);
				// and now if there is optional optimized key buffer data ... 
				// append optimized key data buffer at end 

				if (_optimizedKeyGenerator != null) {
					if (_optimizedKey.getDataBufferSize() != 0) { 
						// update relative positon of data buffer in key  
						_optimizedKey.setDataBufferOffset(_spillDataBuffer.position() - startPositon);
						// write buffer to spill buffer 
						_optimizedKey.writeBufferToStream(_outputStream);
					}
					// ok now record final data position
					int nextItemPosition = _spillDataBuffer.position();
					// seek back to begining ...
					_spillDataBuffer.position(startPositon);
					// rewrite header 
					_optimizedKey.writeHeaderToStream(_outputStream);
					// now back to next item position 
					_spillDataBuffer.position(nextItemPosition);
				}
				// store start position in index buffer 
				_spillIndexBuffer[_spillItemCount] = startPositon;
				// increment ... 
				_spillItemCount++;

				//LOG.info("startPos:" + startPositon + " optKeyBufferSize:" + optimizedKeyBufferSize + " keySizePos:" + keySizePos + " keySize:" + keySize + " valueSizePos:" + valueSizePos + " valueSize:" + valueSize);

				done = true;
			}
			// trap for buffer overflow specifically
			catch (IllegalArgumentException e) { 
				//LOG.info("IllegalArgumentException - Buffer Overflow detected while writing to Spill Data Buffer. Flushing");
				overflow = true;
			}
			catch (BufferOverflowException e) {
				//LOG.info("BufferOverflowException - Buffer Overflow detected while writing to Spill Data Buffer. Flushing");
				overflow = true;
			}
			if (overflow) { 
				// reset to start position 
				_spillDataBuffer.position(startPositon);
				// sort and spill 
				sortAndSpill(null);
			}
		}
	}

	private static OutputStream newOutputStream(final ByteBuffer buf) {
		return new OutputStream() {

			@Override
			public void write(int b) throws IOException {
				buf.put((byte) (b & 0xff));
			}

			public void write(byte src[], int off, int len) throws IOException {
				buf.put(src, off, len);
			}
		};  
	}

	public static InputStream newInputStream(final ByteBuffer buf) {
		return new InputStream() {
			public synchronized int read() throws IOException {
				if (!buf.hasRemaining()) {
					return -1;
				}
				return buf.get() & 0xff;
			}

			public synchronized int read(byte[] bytes, int off, int len) throws IOException {
				// Read only what's left
				len = Math.min(len, buf.remaining());
				buf.get(bytes, off, len);
				return len;
			}
		};
	}

}
