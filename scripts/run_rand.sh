#!/bin/bash

./bin/spark-submit -v \
--num-executors 8 --executor-cores 8 --executor-memory 16G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.RandomForestClassifierExample \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar \
2>&1 | tee randomforestlog 

