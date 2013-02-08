#!/usr/bin/env bash

# resolve links - $0 may be a softlink

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done


# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the app installation
export CCAPP_HOME=`dirname "$this"`/..
export CCAPP_CONF_DIR=$CCAPP_HOME/conf
export CCAPP_LOG_DIR=$CCAPP_HOME/logs
export CCAPP_LIB_DIR=$CCAPP_HOME/lib

echo "CCAPP_HOME:"$CCAPP_HOME
echo "CCAPP_CONF_DIR:$CCAPP_CONF_DIR"
echo "CCAPP_LOG_DIR:$CCAPP_LOG_DIR"

if ! [ -e $CCAPP_HOME/target/commoncrawl-*.jar ]; then
	echo "Please build commoncrawl jar"
else
	CCAPP_JAR=`basename $CCAPP_HOME/target/commoncrawl*.jar`
	CCAPP_JAR_PATH=$CCAPP_HOME/target
	echo "CCAPP_JAR:"$CCAPP_JAR
	echo "CCAPP_JAR_PATH:"$CCAPP_JAR_PATH
fi	

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
else 
	echo "JAVA_HOME:$JAVA_HOME"
fi


if [ "$HADOOP_HOME" = "" ]; then
	echo "HADOOP_HOME not defined. Attempting to locate via build.properties"
	export HADOOP_HOME=`cat $CCAPP_HOME/build.properties | grep "hadoop.path" | sed 's/.*=\(.*\)$/\1/'`
	
	if ! [ "$HADOOP_HOME" = "" ]; then
		echo "Derived HADOOP_HOME from build.properties to be:$HADOOP_HOME"
	else
		echo "Failed to extract HADOOP_HOME from build.properties. Please set HADOOP_HOME to point to Hadoop Distribution"
		exit 1
	fi
fi

if [ -f $HADOOP_HOME/build/hadoop-*-core.jar ]; then 
    HADOOP_JAR=`ls $HADOOP_HOME/build/hadoop-*-core.jar`
elif [ -f $HADOOP_HOME/hadoop-*-core.jar ]; then
    HADOOP_JAR=`ls $HADOOP_HOME/hadoop-*-core.jar`
elif [ -f "${HADOOP_HOME}/build/hadoop-core-*.jar" ]; then
    HADOOP_JAR=`ls $HADOOP_HOME/build/hadoop-core-*.jar`
elif [ -f ${HADOOP_HOME}/hadoop-core-*.jar ]; then
    HADOOP_JAR=`ls $HADOOP_HOME/hadoop-core-*.jar`
fi

if [ "$HADOOP_JAR" = "" ]; then
  echo "Unable to locate hadoop core jar file. Please check your hadoop installation."
  exit 1
else
  echo HADOOP JAR IS:${HADOOP_JAR}
fi

if [ "$HADOOP_CONF_DIR" = "" ]; then
	HADOOP_CONF_DIR="$HADOOP_HOME/conf"
fi

echo "HADOOP_JAR:$HADOOP_JAR"
echo "HADOOP_CONF_DIR:$HADOOP_CONF_DIR"





# CLASSPATH initially contains CCAPP_CONF:HADOOP_CONF_DIR
CLASSPATH=${CCAPP_CONF_DIR}
# add in hadoop config
CLASSPATH=${CLASSPATH}:${HADOOP_CONF_DIR}
# and hbase config
#CLASSPATH=${CLASSPATH}:${HBASE_CONF_DIR}
# add in web app class path 
CLASSPATH=${CLASSPATH}:${CCAPP_HOME}/webapps
# and add in test path ... 
CLASSPATH=${CLASSPATH}:${CCAPP_HOME}/tests
# next add tools.jar
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
# next add commoncrawl jar FIRST ... 
CLASSPATH=${CLASSPATH}:${CCAPP_JAR_PATH}/${CCAPP_JAR}
# then add nested libraries in commoncrawl jar
for f in ${CCAPP_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
# then add nested libraries in commoncrawl jar
for f in ${CCAPP_HOME}/target/classes/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
#next add hadoop jar path 
CLASSPATH=${CLASSPATH}:${HADOOP_JAR}
# add hadoop libs to CLASSPATH
for f in $HADOOP_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done
#next add hbase jar path 
#CLASSPATH=${CLASSPATH}:${HBASE_JAR}
# add hbase libs to CLASSPATH
#for f in $HBASE_HOME/lib/*.jar; do
#  CLASSPATH=${CLASSPATH}:$f;
#done
# and add jetty libs ... 
for f in $HADOOP_HOME/lib/jetty-ext/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

echo "";
echo "CLASSPATH:$CLASSPATH"
echo "";

CCAPP_CLASS_NAME=$1

if [ "$CCAPP_CLASS_NAME" = "" ]; then
	echo "No Main Class Specified!"
	exit 1;
fi

echo "CCAPP_CLASS_NAME:$CCAPP_CLASS_NAME"
CCAPP_NAME=`echo $CCAPP_CLASS_NAME | sed 's/.*\.\(.*\)$/\1/'`
echo "CCAPP_NAME:$CCAPP_NAME"

if [ "$JAVA_HEAP_MAX" = "" ]; then
	JAVA_HEAP_MAX=-Xmx2000m 
fi

JAVA="$JAVA_HOME/bin/java"

#establish hadoop platform name string 
JAVA_PLATFORM=`CLASSPATH=${CLASSPATH} ${JAVA} org.apache.hadoop.util.PlatformName | sed -e 's/ /_/g' | sed -e "s/ /_/g"`
echo Platform Name is:${JAVA_PLATFORM}
#setup commoncrawl library paths
JAVA_LIBRARY_PATH=${CCAPP_LIB_DIR}:${CCAPP_LIB_DIR}/native/${JAVA_PLATFORM}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}
#setup execution path 
export PATH=${CCAPP_LIB_DIR}/native/${JAVA_PLATFORM}:$PATH
#and ld_library path 
export LD_LIBRARY_PATH=${CCAPP_LIB_DIR}/native/${JAVA_PLATFORM}:${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}:$LD_LIBRARY_PATH
echo LD_LIBRARY_PATH: ${LD_LIBRARY_PATH}
if [ $CCAPP_NAME = "CommonCrawlServer" ] ; then
	GET_REALAPP_NAME_CMD="$JAVA -classpath $CLASSPATH $CCAPP_CLASS $@ --dumpAppName"
	CCAPP_NAME=`$GET_REALAPP_NAME_CMD`
	echo "Real app name is:" $CCAPP_NAME
fi

if [ -z $CCAPP_NAME ]; then
	echo "Unable to retrieve app name!"
	exit 1
fi
CCAPP_LOG_FILE=$CCAPP_NAME.log
	
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.log.dir=$CCAPP_LOG_DIR"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.log.file=$CCAPP_LOG_FILE"
CCAPP_VMARGS="$CCAPP_VMARGS -Dhadoop.home.dir=$HADOOP_HOME"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcommoncrawl.root.logger=${CCAPP_ROOT_LOGGER:-INFO,DRFA}"
CCAPP_VMARGS="$CCAPP_VMARGS $JAVA_HEAP_MAX"
CCAPP_VMARGS="$CCAPP_VMARGS -XX:+UseParNewGC -XX:ParallelGCThreads=8 -XX:NewSize=200m -XX:+PrintGCDetails"
CCAPP_VMARGS="$CCAPP_VMARGS -Djava.library.path=${JAVA_LIBRARY_PATH}"
CCAPP_VMARGS="$CCAPP_VMARGS -Dcc.native.lib.path=${CCAPP_LIB_DIR}/native/${JAVA_PLATFORM}"

CCAPP_CMD_LINE="$JAVA $CCAPP_VMARGS -classpath $CLASSPATH $CCAPP_CLASS $@"
CCAPP_RUN_LOG=$CCAPP_LOG_DIR/${CCAPP_NAME}_run.log
$CCAPP_CMD_LINE "$@" | tee $CCAPP_RUN_LOG



