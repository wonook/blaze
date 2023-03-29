#!/bin/bash

./bin/spark-submit -v \
--num-executors 2 --executor-cores 8 --executor-memory 50G --driver-memory 16G \
--master yarn --class org.apache.spark.examples.graphx.SVDPlusPlusExample \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar \
/svdpp-medium \
2>&1 | tee spark_log.txt
