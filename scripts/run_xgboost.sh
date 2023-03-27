#!/bin/bash
XGBOOST_SPARK_JAR="/home/wonook/xgboost/jvm-packages/xgboost4j-spark/target/xgboost4j-spark_2.12-1.0.0-SNAPSHOT.jar"

./bin/spark-submit -v \
--num-executors 6 --executor-cores 4 --executor-memory 16G --driver-memory 1G \
--master yarn --class ml.dmlc.xgboost4j.scala.example.spark.SparkTraining \
--conf "spark.driver.extraClassPath=$XGBOOST_SPARK_JAR" \
--conf "spark.executor.extraClassPath=$XGBOOST_SPARK_JAR" \
/home/wonook/xgboost/jvm-packages/xgboost4j-example/target/xgboost4j-example_2.12-1.0.0-SNAPSHOT.jar \
iris.data \
2>&1 | tee log_xgboost
