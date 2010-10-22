package org.commoncrawl.samples;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.commoncrawl.ec2jobs.ARCFileMetadataExtractor;
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.ARCResource;
import org.commoncrawl.hadoop.io.ARCSplit;
import org.commoncrawl.hadoop.io.JetS3tARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.protocol.shared.ArcFileMetadata;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.FileUtils;

import com.hadoop.compression.lzo.LzoCodec;

public class BasicArcFileReaderSample {

  /** logging **/
  private static final Log LOG = LogFactory.getLog(ARCFileMetadataExtractor.class);

	
  public static void main(String[] args) {
	  
		String awsCredentials = args[0];
		String awsSecret      = args[1];
		String bucketName  		= args[2];
		String inputPrefixes  = args[3];
		
		JobConf conf = new JobConf();
		
		System.out.println("using awsCredentials:" + awsCredentials);
		conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
		conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecret);
		System.out.println("using bucket:" + bucketName);
		conf.set(JetS3tARCSource.P_BUCKET_NAME,bucketName);
		System.out.println("using input prefixes:" + inputPrefixes);
		conf.set(JetS3tARCSource.P_INPUT_PREFIXES, inputPrefixes);
		
		// crete input format 
		ARCInputFormat inputFormat = new ARCInputFormat();
		// configure it 
		inputFormat.configure(conf);
		
		// create a tmp file 
		File tmpFile = new File("/tmp/ExtractorTest");
		FileUtils.recursivelyDeleteFile(tmpFile);
		tmpFile.mkdir();
		
		// get splits ... 
		try {
			
	    InputSplit[] splits = inputFormat.getSplits(conf, 0);
	    
	    
	    if (splits.length == 0) { 
	    	System.out.println("ERROR: No Arc Files Found!");
	    	return;
	    }
	    System.out.println("Found " + splits.length + " InputSplits:");
	    // dump splits ... 
	    for (InputSplit split : splits) { 
	    	System.out.println("Processing Split:" + split.toString());
	    	
	    	ARCResource resource = ((ARCSplit)split).getResources()[0];
	    	Path arcFileName = new Path(resource.getName());
	    	Path   outputPath = new Path(tmpFile.getAbsolutePath(),arcFileName.getName().substring(0,arcFileName.getName().lastIndexOf(".gz")) + ".seq");
	    	FileSystem localFS = FileSystem.getLocal(conf);
	    	
	    	System.out.println("Local file will be:" + outputPath);

	    	// create a local sequence file
    		SequenceFile.Writer writer = SequenceFile.createWriter(localFS,conf, outputPath, Text.class, ArcFileItem.class, CompressionType.BLOCK, new LzoCodec());
	    	
	    	try { 
		    	// create reader ... 
		    	RecordReader reader = inputFormat.getRecordReader(split, conf, new Reporter() {
	
						@Override
	          public Counter getCounter(Enum<?> arg0) {
		          return null;
	          }
	
						@Override
	          public Counter getCounter(String arg0, String arg1) {
		          // TODO Auto-generated method stub
		          return null;
	          }
	
						@Override
	          public InputSplit getInputSplit()
	              throws UnsupportedOperationException {
		          // TODO Auto-generated method stub
		          return null;
	          }
	
						@Override
	          public void incrCounter(Enum<?> arg0, long arg1) {
							System.out.println("Incementing:" + arg0.toString());
	          }
	
						@Override
	          public void incrCounter(String arg0, String arg1, long arg2) {
		          // TODO Auto-generated method stub
		          
	          }
	
						@Override
	          public void setStatus(String arg0) {
		          // TODO Auto-generated method stub
		          
	          }
	
						@Override
	          public void progress() {
		          // TODO Auto-generated method stub
		          
	          } 
		    		
		    	});
		    	
		    	Text url = new Text();
		    	ArcFileItem item = new ArcFileItem();
		    	
		    	while (reader.next(url, item)) { 
		    		// echo back to sequence file for test purposes
		    		writer.append(url, item);
		    		// and output url ... 
		    		System.out.println("URL:" + url.toString());
		    	}
	    	}
	    	finally { 
	    		writer.close();
	    	}
	    }
		}
		catch (IOException e) { 
			LOG.error(CCStringUtils.stringifyException(e));
		}
	}
}