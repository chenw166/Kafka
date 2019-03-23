#!/bin/bash
# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

base_dir=$(cd "$(dirname "$0")"; pwd)
for file in $base_dir/libs/*.jar;
do
  CLASSPATH=$CLASSPATH:$file
done
CLASSPATH=.:$base_dir/config:$CLASSPATH
$JAVA -cp $CLASSPATH com.huawei.dms.kafka.DmsKafkaProduceDemo