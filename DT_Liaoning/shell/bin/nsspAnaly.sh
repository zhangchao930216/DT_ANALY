#!/bin/bash
export JAVA_HOME=/opt/app/java
export HADOOP_HOME=/opt/app/hadoop
export SPARK_HOME=/opt/app/spark
export HADOOP_CONF_DIR=/opt/app/hdconf
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin::$SPARK_HOME/bin
ANALY_DATE=$1
ANALY_HOUR=$2
MAIN_CLASS=com.dtmobile.spark.job.AnalyJob
JAR=/dt/lib/DT_Liaoning-1.0-SNAPSHOT.jar
MASTER=spark://192.168.3.2:7077

/opt/app/spark/bin/spark-submit  \
 --class $MAIN_CLASS \
 --executor-memory 5G \
 --executor-cores 3 \
 --jars /dt/lib/ojdbc6-12.1.0.2.jar \
 $JAR $ANALY_DATE $ANALY_HOUR default dcl $MASTER 192.168.3.14:1521/umorpho datang 0


#if [ $ANALY_DATE = 00 ];then
# sh /dt/bin/nsspAnaly1.sh
#fi