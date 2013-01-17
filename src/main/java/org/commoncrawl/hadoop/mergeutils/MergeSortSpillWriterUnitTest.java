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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.commoncrawl.util.shared.CCStringUtils;

/**
 * A bunch of unit tests covering possible combinations of comparators.
 * 
 * @author rana
 * 
 */
public class MergeSortSpillWriterUnitTest {

    public static final Log LOG = LogFactory
        .getLog(MergeSortSpillWriterUnitTest.class);

    static abstract class BaseTest {

        TreeMap<Integer, Text> originalKeyValueMap = new TreeMap<Integer, Text>();
        int index[];
        String _testName = null;
        int _keySetSize = -1;
        int _indexBufferSize = -1;
        int _dataBufferSize = -1;
        int _spillBufferSize = -1;

        public BaseTest(String testName, int keySetSize, int indexBufferSize,
            int dataBufferSize, int spillBufferSize) {
            _testName = testName;
            _keySetSize = keySetSize;
            _indexBufferSize = indexBufferSize;
            _dataBufferSize = dataBufferSize;
            _spillBufferSize = spillBufferSize;
        }

        public void runTest() throws IOException {
            LOG.info("*************** STARTING TEST:" + _testName);
            LOG.info("Set Size:" + _keySetSize);
            LOG.info("Index Buffer Size:" + _indexBufferSize);
            LOG.info("Data Buffer Size:" + _dataBufferSize);
            LOG.info("Spill Buffer Size:" + _spillBufferSize);
            LOG.info("");

            long testStartTime = System.currentTimeMillis();

            // initialization here
            // create an array of keys and an index into them ...
            index = new int[_keySetSize];
            for (int i = 0; i < _keySetSize; ++i) {
                index[i] = i;
                originalKeyValueMap.put(i, new Text(keyForNumber(i)));
            }
            // randomly shuffle the index
            Random r = new Random();
            // Shuffle array
            for (int i = index.length; i > 1; i--)
                swap(index, i - 1, r.nextInt(i));

            // ok create a spill writer that validates position and value
            RawDataSpillWriter<IntWritable, Text> validatingSpillWriter = new RawDataSpillWriter<IntWritable, Text>() {

                int closeCount = 0;

                // initial spill count to zero
                int spilledKeyCount = 0;

                @Override
                public void close() throws IOException {
                    if (++closeCount > 1) {
                        throw new IOException("Close Called One Too Many Times!");
                    }
                    if (spilledKeyCount != index.length) {
                        throw new IOException("Spilled Key Count:" + spilledKeyCount
                            + " Excpected Key Count:" + index.length);
                    }
                }

                @Override
                public void spillRecord(IntWritable key, Text value) throws IOException {
                    // LOG.info("Got Key:" + key.get() + " Value:"+ value);
                    // if keys don't match ...
                    if (key.get() != spilledKeyCount) {
                        throw new IOException("Got Key:" + key.get() + " Expected Key:"
                            + spilledKeyCount);
                    }
                    // ok keys match... check that values match ...
                    Text expectedValue = originalKeyValueMap.get(spilledKeyCount);
                    // ok validate expected value ..
                    if (expectedValue == null || value == null) {
                        throw new IOException("Null Expected or Incoming Value");
                    } else {
                        if (expectedValue.compareTo(value) != 0) {
                            throw new IOException("Expected Value:" + expectedValue
                                + " @index:" + spilledKeyCount
                                + " differs from resulting value:" + value);
                        }
                    }
                    spilledKeyCount++;
                }

                DataInputBuffer keyReader = new DataInputBuffer();
                DataInputBuffer valueReader = new DataInputBuffer();
                IntWritable keyObject = new IntWritable();
                Text valueObject = new Text();

                @Override
                public void spillRawRecord(byte[] keyData, int keyOffset,
                    int keyLength, byte[] valueData, int valueOffset, int valueLength)
                    throws IOException {
                    // LOG.info("Got Raw Record");
                    // initialize key / value readers .
                    keyReader.reset(keyData, keyOffset, keyLength);
                    valueReader.reset(valueData, valueOffset, valueLength);
                    keyObject.readFields(keyReader);
                    valueObject.readFields(valueReader);
                    this.spillRecord(keyObject, valueObject);
                }
            };

            // create a local file system
            Configuration conf = new Configuration();

            // create a raw comparator
            RawKeyValueComparator<IntWritable, Text> comparator = new RawKeyValueComparator<IntWritable, Text>() {

                DataInputBuffer keyReader1 = new DataInputBuffer();
                DataInputBuffer keyReader2 = new DataInputBuffer();

                @Override
                public int compareRaw(byte[] key1Data, int key1Offset, int key1Length,
                    byte[] key2Data, int key2Offset, int key2Length, byte[] value1Data,
                    int value1Offset, int value1Length, byte[] value2Data,
                    int value2Offset, int value2Length) throws IOException {

                    keyReader1.reset(key1Data, key1Offset, key1Length);
                    keyReader2.reset(key2Data, key2Offset, key2Length);

                    return ((Integer) keyReader1.readInt()).compareTo(keyReader2
                        .readInt());
                }

                @Override
                public int compare(IntWritable key1, Text value1, IntWritable key2,
                    Text value2) {
                    return key1.compareTo(key2);
                }
            };

            // setup conf

            // number of records to store in RAM before doing an intermediate sort
            conf.setInt(MergeSortSpillWriter.SPILL_INDEX_BUFFER_SIZE_PARAM,
                _indexBufferSize);
            // size of intermediate buffer key value buffer ...
            conf.setInt(MergeSortSpillWriter.SPILL_DATA_BUFFER_SIZE_PARAM,
                _dataBufferSize);
            // set spill write buffer size ...
            conf.setInt(SequenceFileSpillWriter.SPILL_WRITER_BUFFER_SIZE_PARAM,
                _spillBufferSize);

            // ok create the spill writer
            MergeSortSpillWriter<IntWritable, Text> merger = constructMerger(conf,
                validatingSpillWriter, FileSystem.getLocal(conf), new Path("/tmp"),
                comparator, IntWritable.class, Text.class);

            // and finally ... spill the records in random order
            for (int i = 0; i < index.length; ++i) {
                merger.spillRecord(new IntWritable(index[i]), originalKeyValueMap
                    .get(index[i]));
            }
            // ok close merger ...
            merger.close();
            // now close the external spill writer ...
            validatingSpillWriter.close();

            LOG.info("*************** ENDING TEST:" + _testName + " TOOK:"
                + (System.currentTimeMillis() - testStartTime));
        }

        protected abstract MergeSortSpillWriter<IntWritable, Text> constructMerger(
            Configuration conf, RawDataSpillWriter<IntWritable, Text> writer,
            FileSystem tempFileSystem, Path tempFilePath,
            RawKeyValueComparator<IntWritable, Text> comparator, Class keyClass,
            Class valueClass) throws IOException;

        private static final void swap(int[] arr, int i, int j) {
            int tmp = arr[i];
            arr[i] = arr[j];
            arr[j] = tmp;
        }

        private static final String keyForNumber(int number) {
            // establish pattern start location
            int patternStartIdx = number % 26;
            // establish pattern size ...
            int patternSize = (number % 100) + 1;
            // preallocate buffer
            StringBuffer buffer = new StringBuffer(patternSize);
            // build pattern
            int currPatternIdx = patternStartIdx;
            for (int i = 0; i < patternSize; ++i) {
                buffer.append((char) ('A' + currPatternIdx));
                currPatternIdx = (currPatternIdx + 1) % 26;
            }
            return buffer.toString();
        }
    }

    public static class BasicTest extends BaseTest {

        public BasicTest() {
            super("Basic RawKeyValueComparator Test", 1000000, 10000, 10000 * 200,
                1000000);
        }

        @Override
        protected MergeSortSpillWriter<IntWritable, Text> constructMerger(
            Configuration conf, RawDataSpillWriter<IntWritable, Text> writer,
            FileSystem tempFileSystem, Path tempFilePath,
            RawKeyValueComparator<IntWritable, Text> comparator, Class keyClass,
            Class valueClass) throws IOException {

            return new MergeSortSpillWriter<IntWritable, Text>(conf, writer,
                tempFileSystem, tempFilePath, null, comparator, keyClass, valueClass,
                null, null);
        }
    }

    public static class BasicOptimizedTest extends BaseTest {

        public BasicOptimizedTest() {
            super("OptimizedKeyGenerator - using Long ONLY Keys Test", 1000000,
                10000, 10000 * 200, 1000000);
        }

        @Override
        protected MergeSortSpillWriter<IntWritable, Text> constructMerger(
            Configuration conf, RawDataSpillWriter<IntWritable, Text> writer,
            FileSystem tempFileSystem, Path tempFilePath,
            RawKeyValueComparator<IntWritable, Text> comparator, Class keyClass,
            Class valueClass) throws IOException {

            return new MergeSortSpillWriter<IntWritable, Text>(conf, writer,
                tempFileSystem, tempFilePath,
                new OptimizedKeyGeneratorAndComparator<IntWritable, Text>() {

                    @Override
                    public void generateOptimizedKeyForPair(
                        IntWritable key,
                        Text value,
                        org.commoncrawl.hadoop.mergeutils.OptimizedKeyGeneratorAndComparator.OptimizedKey optimizedKeyOut)
                        throws IOException {
                        optimizedKeyOut.setLongKeyValue(key.get());
                    }

                    @Override
                    public int getGeneratedKeyType() {
                        return OptimizedKey.KEY_TYPE_LONG;
                    }

                }, keyClass, valueClass, null, null);
        }
    }

    public static class BasicOptimizedWithLongAndBufferTest extends BaseTest {

        public BasicOptimizedWithLongAndBufferTest() {
            super("OptimizedKeyGenerator - using Long AND Buffer Keys Test", 1000000,
                10000, 10000 * 200, 1000000);
        }

        @Override
        protected MergeSortSpillWriter<IntWritable, Text> constructMerger(
            Configuration conf, RawDataSpillWriter<IntWritable, Text> writer,
            FileSystem tempFileSystem, Path tempFilePath,
            RawKeyValueComparator<IntWritable, Text> comparator, Class keyClass,
            Class valueClass) throws IOException {

            return new MergeSortSpillWriter<IntWritable, Text>(conf, writer,
                tempFileSystem, tempFilePath,
                new OptimizedKeyGeneratorAndComparator<IntWritable, Text>() {

                    @Override
                    public void generateOptimizedKeyForPair(
                        IntWritable key,
                        Text value,
                        org.commoncrawl.hadoop.mergeutils.OptimizedKeyGeneratorAndComparator.OptimizedKey optimizedKeyOut)
                        throws IOException {
                        // set the long to dummy value to force secondary comparator to
                            // trigger
                        optimizedKeyOut.setLongKeyValue(0);
                        // and set the buffer value by first obtaining an output stream
                            // from key object
                        DataOutputStream bufferOutput = optimizedKeyOut
                            .getBufferKeyValueStream();
                        // and then writing into it
                        bufferOutput.writeLong(key.get());
                        // and finally committing it by calling close
                        bufferOutput.close();
                    }

                    @Override
                    public int getGeneratedKeyType() {
                        return OptimizedKey.KEY_TYPE_LONG_AND_BUFFER;
                    }

                    DataInputBuffer key1ReaderStream = new DataInputBuffer();
                    DataInputBuffer key2ReaderStream = new DataInputBuffer();

                    @Override
                    public int compareOptimizedBufferKeys(byte[] key1Data,
                        int key1Offset, int key1Length, byte[] key2Data,
                        int key2Offset, int key2Length) throws IOException {

                        key1ReaderStream.reset(key1Data, key1Offset, key1Length);
                        key2ReaderStream.reset(key2Data, key2Offset, key2Length);
                        return (int) (key1ReaderStream.readLong() - key2ReaderStream
                            .readLong());

                    }

                }, keyClass, valueClass, null, null);
        }
    }

    public static class BasicOptimizedWithBufferOnlyTest extends BaseTest {

        public BasicOptimizedWithBufferOnlyTest(int keySetSize,
            int indexBufferSize, int dataBufferSize, int spillBufferSize) {
            super("OptimizedKeyGenerator - using Buffer ONLY Keys Test", keySetSize,
                indexBufferSize, dataBufferSize, spillBufferSize);
        }

        @Override
        protected MergeSortSpillWriter<IntWritable, Text> constructMerger(
            Configuration conf, RawDataSpillWriter<IntWritable, Text> writer,
            FileSystem tempFileSystem, Path tempFilePath,
            RawKeyValueComparator<IntWritable, Text> comparator, Class keyClass,
            Class valueClass) throws IOException {

            return new MergeSortSpillWriter<IntWritable, Text>(conf, writer,
                tempFileSystem, tempFilePath,
                new OptimizedKeyGeneratorAndComparator<IntWritable, Text>() {

                    @Override
                    public void generateOptimizedKeyForPair(
                        IntWritable key,
                        Text value,
                        org.commoncrawl.hadoop.mergeutils.OptimizedKeyGeneratorAndComparator.OptimizedKey optimizedKeyOut)
                        throws IOException {
                        // and set the buffer value by first obtaining an output stream
                            // from key object
                        DataOutputStream bufferOutput = optimizedKeyOut
                            .getBufferKeyValueStream();
                        // and then writing into it
                        bufferOutput.writeLong(key.get());
                        // and finally committing it by calling close
                        bufferOutput.close();
                    }

                    @Override
                    public int getGeneratedKeyType() {
                        return OptimizedKey.KEY_TYPE_BUFFER;
                    }

                    DataInputBuffer key1ReaderStream = new DataInputBuffer();
                    DataInputBuffer key2ReaderStream = new DataInputBuffer();

                    @Override
                    public int compareOptimizedBufferKeys(byte[] key1Data,
                        int key1Offset, int key1Length, byte[] key2Data,
                        int key2Offset, int key2Length) throws IOException {

                        key1ReaderStream.reset(key1Data, key1Offset, key1Length);
                        key2ReaderStream.reset(key2Data, key2Offset, key2Length);
                        return (int) (key1ReaderStream.readLong() - key2ReaderStream
                            .readLong());

                    }

                }, keyClass, valueClass, null, null);
        }
    }

    public static void main(String[] args) {
        try {
            new BasicTest().runTest();
            new BasicOptimizedTest().runTest();
            new BasicOptimizedWithLongAndBufferTest().runTest();
            new BasicOptimizedWithBufferOnlyTest(1000000, 10000, 10000 * 200, 1000000)
                .runTest();
            new BasicOptimizedWithBufferOnlyTest(1000000, 1000000, 1000000 * 200,
                1000000).runTest();
            // new
            // BasicOptimizedWithBufferOnlyTest(10000000,1000000,1000000*200,1000000).runTest();
        } catch (IOException e) {
            LOG.error(CCStringUtils.stringifyException(e));
        }
    }
}
