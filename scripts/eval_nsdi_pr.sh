#!/bin/bash

# Types
# Blaze-Disagg
# Blaze-Local
# Blaze-Both
# Spark

MEM_OVERHEAD=3g

FRACTION=0.5

TIMEOUT=240


PROMOTE=0.0
MEM_FRAC=0.6

TEST_NAME=PageRank
EXECUTORS=8

DAG_PATH=None
ITER=10

CLASS=com.ibm.crail.benchmarks.Main
JAR="/home/ubuntu/blaze_benchmarks/sql-benchmarks/target/sql-benchmarks-1.0.jar"
#ARGS="-t pagerank -gi 10 -i /user/ubuntu/data/graphx/twitter"
ARGS="-t pagerank -gi 10 -i /twitter"
#ARGS="-t pagerank -gi 10 -i /followers.txt"

SLACK=0
TEST_TYPE=Spark
MEM_SIZE=20g
DISAGG=0g
CORES=3

./drdd_oom.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $TIMEOUT $MEM_FRAC $CORES $ARGS


TEST_NAME=CC
# CC 
CLASS=org.apache.spark.examples.graphx.ConnectedComponentsExample
JAR=/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar
ARGS="/twitter"


#./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $TIMEOUT $MEM_FRAC $DISK_SIZE $ARGS


