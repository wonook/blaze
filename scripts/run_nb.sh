#!/bin/bash

./bin/spark-submit -v \
--num-executors 5 --executor-cores 4 --executor-memory 40G --driver-memory 4G \
--master yarn --class org.apache.spark.examples.ml.NaiveBayesExample \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar \
2>&1 | tee nb_log

