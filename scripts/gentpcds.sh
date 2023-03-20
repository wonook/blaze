SF=300

./bin/spark-submit --num-executors 8 --executor-cores 8 --executor-memory 20g --driver-memory 4g --master yarn \
--class com.ibm.crail.spark.tools.ParquetGenerator \
~/parquet-generator/target/parquet-generator-1.0.jar -c tpcds -o hdfs://anna-03:9000/tpcds-$SF -t $SF -p $SF -tsf $SF -tdsd ~/tpcds-kit/tools \
2>&1 | tee gentpcds_log



