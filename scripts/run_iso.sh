#!/bin/bash

./bin/spark-submit -v \
--num-executors 5 --executor-cores 8 --executor-memory 50G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.IsotonicRegressionExample \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar

