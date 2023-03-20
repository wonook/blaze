#!/bin/bash

./bin/spark-submit -v \
--num-executors 5 --executor-cores 8 --executor-memory 50G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.GradientBoostedTreeRegressorExample \
examples/target/scala-2.12/jars/spark-examples_2.12-3.0.0.jar \
2>&1 | tee gbtclog

