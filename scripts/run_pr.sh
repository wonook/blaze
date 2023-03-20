#!/bin/bash

ITER=$1
INPUT=$2

./bin/spark-submit --num-executors 8 --executor-cores 8 --executor-memory 20g --driver-memory 8g \
--master yarn --class com.ibm.crail.benchmarks.Main \
--conf "spark.app.name=PageRank" \
~/sql-benchmarks/target/sql-benchmarks-1.0.jar \
-t pagerank -gi $ITER -i /$INPUT \
2>&1 | tee pr_spark_iter_$ITER_$INPUT 

#--conf "spark.driver.extraClassPath=$ALLUXIO_JAR:." \
#--conf "spark.executor.extraClassPath=$ALLUXIO_JAR:." \

