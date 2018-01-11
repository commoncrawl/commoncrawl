# Common Crawl Support Library

## Overview

This library provides support code for the consumption of the Common Crawl Corpus
RAW crawl data (ARC Files) stored on S3. More information about how to access the corpus 
can be found at https://commoncrawl.atlassian.net/wiki/display/CRWL/About+the+Data+Set .

You can take two primary routes to consuming the ARC File content: 

(1) You can run a Hadoop cluster on EC2 or use EMR to run a Hadoop job. In this case, 
you can use the ARCFileInputFormat to drive data to your mappers/reducers. There are two 
versions of the InputFormat: One written to conform to the deprecated mapred package, 
located at org.commoncrawl.hadoop.io.mapred and one written for the mapreduce package,
correspondingly located at org.commoncrawl.hadoop.io.mapreduce. 

(2) You can decode data directly by feeding an InputStream to the ARCFileReader class 
located in the org.commoncrawl.util.shared package. 

Both routes (InputFormat or ARCFileReader direct route) produce a tuple consisting of a UTF-8 
encoded URL (Text), and the raw content (BytesWritable), including HTTP headers, that were 
downloaded by the crawler. The HTTP headers are UTF-8 encoded, and the headers and content
are delimited by a consecutive set of CRLF tokens. The content itself, when it is of a text
mime type, is encoded using the source text encoding. 

## Build Notes: 

1. You need to define JAVA_HOME, and make sure you have Ant & Maven installed.
2. Set hadoop.path (in build.properties) to point to your Hadoop distribution.

# Sample Usage:

Once the commoncrawl.jar has been built, you can validate that the ARCFileReader works for you by 
executing the sample command line from root for the commoncrawl source directory:  

	./bin/launcher.sh org.commoncrawl.util.shared.ARCFileReader --awsAccessKey <ACCESS KEY> --awsSecret <SECRET> --file s3n://aws-publicdatasets/common-crawl/parse-output/segment/1341690164240/1341819847375_4319.arc.gz
