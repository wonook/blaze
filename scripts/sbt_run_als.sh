#!/bin/bash

./bin/spark-submit -v \
--num-executors 6 --executor-cores 4 --executor-memory 32G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.ALSExample \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar \
2>&1 | tee als_console_vanilla
