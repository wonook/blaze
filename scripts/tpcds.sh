#!/bin/bash
# Queries with join ops: 28, 40, 49, 51, 61, 72, 77, 78, 80, 88, 90, 93, 97
for i in 72 # 4 16 64 95 
do
#hdfs dfs -rm -R /tpcds-output

./bin/spark-submit -v \
	--num-executors 8 --executor-cores 8 --executor-memory 30g --driver-memory 4g --master yarn \
	--conf "spark.app.name=tpcds-q${i}" \
	--class com.ibm.crail.benchmarks.Main \
	~/sql-benchmarks/target/sql-benchmarks-1.0.jar \
	-t q${i} -i /tpcds-100 -t tpcds -of parquet \
	2>&1 | tee tpcds_log 
done
