#!/bin/bash

./bin/spark-submit -v \
--num-executors 12 --executor-cores 2 --executor-memory 8G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.MulticlassLogisticRegressionWithElasticNetExample \
--conf "spark.memory.storageFraction=0.2" \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar \
2>&1 | tee mlrlogs_default

