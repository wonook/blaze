#!/bin/bash

./bin/spark-submit -v \
--num-executors 5 --executor-cores 8 --executor-memory 50G --driver-memory 4G \
--master yarn --class org.apache.spark.examples.ml.ModelSelectionViaCrossValidationExample \
/home/jyeo/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar \
2>&1 | tee example_log

