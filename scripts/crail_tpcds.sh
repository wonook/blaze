#!/bin/bash
# Queries with join ops: 28, 40, 49, 51, 61, 72, 77, 78, 80, 88, 90, 93, 97
for i in 72 # 4 16 64 95 
do
#hdfs dfs -rm -R /tpcds-output

./bin/spark-submit -v \
	--num-executors 4 --executor-cores 8 --executor-memory 8G --driver-memory 10G --master yarn \
	--conf "spark.app.name=tpcds-q${i}" \
	--conf "spark.driver.extraClassPath=$CRAIL_JAR/*:." \
	--conf "spark.executor.extraClassPath=$CRAIL_JAR/*:." \
	--conf "spark.disagg.evictpolicy=DRDD" \
	--class com.ibm.crail.benchmarks.Main \
	~/sql-benchmarks/target/sql-benchmarks-1.0.jar \
	-t q${i} -i /tpcds-300 -t tpcds -of parquet \
	2>&1 | tee crail_tpcds_log 
done
