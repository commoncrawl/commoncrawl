#CommonCrawl Support Library

##Overview

The commoncrawl source code repository is used as a distribution vehicle for our custom
Hadoop InputFormat (ARCInputFormat located in org.commoncrawl.hadoop.io). Please refer 
to the CommonCrawl website at http://www.commoncrawl.org/ for more details on how to 
access our crawl corpus.

The sample class BasicArcFileReaderSample.java (located in org.commoncrawl.samples) for an 
example of how to configure the InputFormat. A more detailed example of how to use it in 
the context of a Hadoop Job will be forthcoming.

##Build Notes: 

1. You need to define JAVA_HOME, and make sure you have Ant & Maven installed.
2. Set hadoop.path (in build.properties) to point to your Hadoop folder.
3. Make sure you have the thrift compiler (version 0.7.0) installed on your system.
4. If you want to use the Google URL Canoncilization library in Hadoop job,
   copy the shared libraries under lib/native/{Platform} to /usr/local/lib or equivalent.

#Sample Usage:

Once commoncrawl.jar has been built, you can execute a job/sample via the bin/launcher.sh script.
The sample class BasicArcFileReaderSample.java (located in org.commoncrawl.samples) demonstrates  
how you can go about configuring our InputFormat. To run the BasicArcFileReaderSample against
an ARC file in the corpus (2010/01/07/18/1262876244253_18.arc.gz for example), you would run 
the following command line:

  bin/launcher.sh org.commoncrawl.samples.BasicArcFileReaderSample <<AWS ACCESS KEY>>  <<AWS SECRET KEY>> commoncrawl-crawl-002 2010/01/07/18/1262876244253_18.arc.gz


