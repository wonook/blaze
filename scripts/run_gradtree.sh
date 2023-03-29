#!/bin/bash

./bin/spark-submit -v \
--num-executors 6 --executor-cores 4 --executor-memory 16G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.GradientBoostedTreeClassifierExample \
--conf "spark.memory.storageFraction=0.2" \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar \
2 1 1 true colon-cancer-small \
2>&1 | tee gradtreelog

