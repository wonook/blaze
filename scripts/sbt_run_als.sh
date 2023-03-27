#!/bin/bash

./bin/spark-submit -v \
--num-executors 6 --executor-cores 4 --executor-memory 32G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.ALSExample \
/home/wonook/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar \
2>&1 | tee als_console_vanilla
