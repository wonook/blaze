#!/bin/bash
# Queries with join ops: 28, 40, 49, 51, 61, 72, 77, 78, 80, 88, 90, 93, 97
#hdfs dfs -rm -R /tpcds-output

./bin/spark-submit -v \
--num-executors 4 --executor-cores 4 --executor-memory 4G --driver-memory 1G \
--master yarn --class com.ibm.crail.benchmarks.Main ~/sql-benchmarks/target/sql-benchmarks-1.0.jar \
-t q$1 -i /tpcds-100 -t tpcds \
-a save,/tpcds-output-$1/ \
-of parquet  \
&> ./logs/$1_log

