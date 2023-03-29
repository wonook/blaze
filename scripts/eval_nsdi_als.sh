#!/bin/bash

# Types
# Blaze-Disagg
# Blaze-Local
# Blaze-Both
# Spark

MEM_OVERHEAD=4g

FRACTION=0.5

TIMEOUT=240


PROMOTE=0.0
MEM_FRAC=0.6

EXECUTORS=8

DAG_PATH=None
ITER=10

CLASS=com.ibm.crail.benchmarks.Main
JAR="/home/ubuntu/blaze_benchmarks/sql-benchmarks/target/sql-benchmarks-1.0.jar"
#ARGS="-t pagerank -gi 10 -i /user/ubuntu/data/graphx/twitter"
#ARGS="-t pagerank -gi 10 -i /twitter"
ARGS="-t pagerank -gi 10 -i /followers.txt"

SLACK=0
TEST_TYPE=Spark
MEM_SIZE=15g
DISAGG=0g

TEST_NAME=ALS
# CC 
CLASS=org.apache.spark.examples.ml.ALSExample
JAR=/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar
ARGS="none"

./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $TIMEOUT $MEM_FRAC $ARGS


#./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $TIMEOUT $MEM_FRAC $DISK_SIZE $ARGS


