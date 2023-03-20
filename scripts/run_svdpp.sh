#!/bin/bash

./bin/spark-submit -v \
--num-executors 2 --executor-cores 8 --executor-memory 50G --driver-memory 16G \
--master yarn --class org.apache.spark.examples.graphx.SVDPlusPlusExample \
/home/jyeo/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar \
/svdpp-medium \
2>&1 | tee spark_log.txt
