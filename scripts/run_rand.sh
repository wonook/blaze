#!/bin/bash

./bin/spark-submit -v \
--num-executors 8 --executor-cores 8 --executor-memory 16G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.RandomForestClassifierExample \
/home/jyeo/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar \
2>&1 | tee randomforestlog 

