package org.commoncrawl.hadoop.emr.sample;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.commoncrawl.hadoop.io.mapred.ARCFileInputFormat;
import org.commoncrawl.util.shared.ByteArrayUtils;
import org.commoncrawl.util.shared.CCStringUtils;
import org.commoncrawl.util.shared.JobBuilder;

public class BareBonesJob implements Mapper<Text, BytesWritable, NullWritable, NullWritable>{

  /** logging **/
  private static final Log LOG = LogFactory.getLog(BareBonesJob.class);
      
  static Options options = new Options();
  static { 
    
    options.addOption(
        OptionBuilder.withArgName("path").hasArgs().withDescription("Input Path").create("path"));
    
  }
  
  static void printUsage() { 
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "BareBonesJob", options );
  }
  
  public static void main(String[] args) {
    CommandLineParser parser = new GnuParser();
    
    try { 
      // parse the command line arguments
      CommandLine cmdLine = parser.parse( options, args );
      
      JobBuilder builder = new JobBuilder("BareBones Job", new Configuration());
      
      LOG.info("Paths:"+ cmdLine.getOptionValues("path"));
      if (cmdLine.hasOption("path")) { 
        for (String path : cmdLine.getOptionValues("path")){
          LOG.info("Adding Input Path:" + path);
          builder.input(new Path(path));
        }
      }
      else { 
        throw new IOException("No Paths Specified!");
      }
      
      builder.keyValue(NullWritable.class, NullWritable.class);
      builder.mapper(BareBonesJob.class);
      builder.inputFormat(ARCFileInputFormat.class);
      builder.outputFormat(NullOutputFormat.class);
      builder.numReducers(0);
      
      JobConf job = builder.build();
      
      JobClient.runJob(job);
      
    }
    catch (IOException e) { 
      LOG.error(CCStringUtils.stringifyException(e));
      System.exit(1);
    }
    catch (ParseException e) { 
      System.out.println(e.toString());
      printUsage();
      System.exit(1);
    }
  }

  @Override
  public void configure(JobConf job) {
    LOG.info("Source File:" + job.get("map.input.file"));
  }

  @Override
  public void close() throws IOException {
    
  }

  @Override
  public void map(Text key, BytesWritable value,OutputCollector<NullWritable, NullWritable> output, Reporter reporter)throws IOException {
    int indexOfTrailingCRLF = ByteArrayUtils.indexOf(value.getBytes(), 0, value.getLength(), "\r\n\r\n".getBytes());
    int headerLen = indexOfTrailingCRLF + 4;
    int contentLen = value.getLength() - headerLen;
    
    String outputStr = "Key:" + key.toString() + " HeaderLen:" + headerLen + " ContentLen:" + contentLen;
    System.out.println(outputStr);
  }
}
