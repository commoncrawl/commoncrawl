package org.commoncrawl.util.shared;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.file.tfile.Utils;
import org.apache.hadoop.io.file.tfile.Utils.Version;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/** TFile helpers
 * 
 * @author rana
 *
 */
public class TFileUtils {

  
  static final Log LOG = LogFactory.getLog(TFileUtils.class);
  
  /**
   * HACKS necessary to get at BCFile Metadata Information :-(
   */
  static final Version BCFILE_API_VERSION = new Version((short) 1, (short) 0);
  final static String TFILE_META_BLOCK_NAME = "TFile.meta";
  final static String TFILE_BLOCK_NAME = "TFile.index";
  final static String BCFILE_META_BLOCK_NAME = "BCFile.index";



  /**
   * Magic number uniquely identifying a BCFile in the header/footer.
   */
  static final class BCFileMagic {
    private final static byte[] AB_MAGIC_BCFILE =
        {
            // ... total of 16 bytes
            (byte) 0xd1, (byte) 0x11, (byte) 0xd3, (byte) 0x68, (byte) 0x91,
            (byte) 0xb5, (byte) 0xd7, (byte) 0xb6, (byte) 0x39, (byte) 0xdf,
            (byte) 0x41, (byte) 0x40, (byte) 0x92, (byte) 0xba, (byte) 0xe1,
            (byte) 0x50 };

    public static void readAndVerify(DataInput in) throws IOException {
      byte[] abMagic = new byte[size()];
      in.readFully(abMagic);

      // check against AB_MAGIC_BCFILE, if not matching, throw an
      // Exception
      if (!Arrays.equals(abMagic, AB_MAGIC_BCFILE)) {
        throw new IOException("Not a valid BCFile.");
      }
    }

    public static void write(DataOutput out) throws IOException {
      out.write(AB_MAGIC_BCFILE);
    }

    public static int size() {
      return AB_MAGIC_BCFILE.length;
    }
  } 
  public static class BCFileMetaReader { 
    public Version version = null;
    public BCFileMetaIndex metaIndex = null;
    public long fileLength;
    
    public BCFileMetaReader(FSDataInputStream fin, long fileLength,
        Configuration conf) throws IOException {

      this.fileLength = fileLength;
      // move the cursor to the beginning of the tail, containing: offset to the
      // meta block index, version and magic
      fin.seek(fileLength - BCFileMagic.size() - Version.size() - Long.SIZE
          / Byte.SIZE);
      long offsetIndexMeta = fin.readLong();
      version = new Version(fin);
      BCFileMagic.readAndVerify(fin);

      if (!version.compatibleWith(BCFILE_API_VERSION)) {
        throw new RuntimeException("Incompatible BCFile fileBCFileVersion.");
      }

      // read meta index
      fin.seek(offsetIndexMeta);
      metaIndex = new BCFileMetaIndex(fin);
    }
    
    
    public long getEarliestMetadataOffset() {
      long earliestIndexPos = fileLength;
      for (Map.Entry<String, BCFileMetaIndexEntry> entry : metaIndex.index.entrySet()) { 
        earliestIndexPos = Math.min(entry.getValue().getRegion().getOffset(),earliestIndexPos);
      }
      return earliestIndexPos;
    }
  }
  
  /**
   * Index for all Meta blocks.
   */
  static class BCFileMetaIndex {
    // use a tree map, for getting a meta block entry by name
    final Map<String, BCFileMetaIndexEntry> index;

    // for write
    public BCFileMetaIndex() {
      index = new TreeMap<String, BCFileMetaIndexEntry>();
    }

    // for read, construct the map from the file
    public BCFileMetaIndex(DataInput in) throws IOException {
      int count = Utils.readVInt(in);
      index = new TreeMap<String, BCFileMetaIndexEntry>();

      for (int nx = 0; nx < count; nx++) {
        BCFileMetaIndexEntry indexEntry = new BCFileMetaIndexEntry(in);
        index.put(indexEntry.getMetaName(), indexEntry);
      }
    }
  }
  
  /**
   * An entry describes a meta block in the MetaIndex.
   */  
  static final class BCFileMetaIndexEntry {
    
    private final String metaName;
    private final String compressionAlgorithm;
    private final static String defaultPrefix = "data:";

    private final BCFileBlockRegion region;

    public BCFileMetaIndexEntry(DataInput in) throws IOException {
      String fullMetaName = Utils.readString(in);
      if (fullMetaName.startsWith(defaultPrefix)) {
        metaName =
            fullMetaName.substring(defaultPrefix.length(), fullMetaName
                .length());
      } else {
        throw new IOException("Corrupted Meta region Index");
      }

      compressionAlgorithm = Utils.readString(in);
      region = new BCFileBlockRegion(in);
    }
    
    public String getMetaName() {
      return metaName;
    }

    public String getCompressionAlgorithm() {
      return compressionAlgorithm;
    }

    public BCFileBlockRegion getRegion() {
      return region;
    }
    
  }
    
  /**
   * Block region.
   */  
  static final class BCFileBlockRegion  {
    private final long offset;
    private final long compressedSize;
    private final long rawSize;

    public BCFileBlockRegion(DataInput in) throws IOException {
      offset = Utils.readVLong(in);
      compressedSize = Utils.readVLong(in);
      rawSize = Utils.readVLong(in);
    }
    
    public long getOffset() {
      return offset;
    }

    public long getCompressedSize() {
      return compressedSize;
    }

    public long getRawSize() {
      return rawSize;
    }
  }
  
  
  public static void main(String[] args) {
    // setup the basics ... 
    Logger logger = Logger.getLogger("org.commoncrawl");
    logger.setLevel(Level.INFO);
    BasicConfigurator.configure();
    
    Configuration conf = new Configuration();
    
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");    
    
    try {
      // get the file system object ...
      FileSystem fs = FileSystem.get(conf);
      
      // load the specified TFIle
      LOG.info("Loading TFile at:" + args[0]);
      
      Path path = new Path(args[0]);
      
      FileStatus status = fs.getFileStatus(path);
      
      FSDataInputStream in = fs.open(path);
      try {
        LOG.info("Initializing Metadata Reader");
        BCFileMetaReader metaReader = new BCFileMetaReader(in,status.getLen(),conf);
        LOG.info("Earliest Index Offset:" + metaReader.getEarliestMetadataOffset());
        
        BCFileMetaIndex index = metaReader.metaIndex;
        for (Map.Entry<String, BCFileMetaIndexEntry> entry : index.index.entrySet()) { 
          LOG.info("Entry:" + entry.getKey() 
              + " Pos:" + entry.getValue().getRegion().getOffset()
              + " Len:" + entry.getValue().getRegion().getRawSize());
        }
      }
      finally { 
        in.close();
      }
      
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
    }
  }
}
