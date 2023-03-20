QUERY=$1
EXECUTORS=$2
MEM=$3
SF=$4
SYSTEM=$5
DAG_PATH=$6
AQE=$7
BJ_SIZE=$8
DISAGG=0g

# cleanup 
parallel-ssh -h ~/compute-hosts.txt 'rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/compute-hosts.txt 'rm -rf /dev/hugepages/data/*' 
parallel-ssh -h ~/compute-hosts.txt 'rm -rf /home/jyeo/xvdb/yarn/*'

# restart crail 
if [ "$SYSTEM" != "Spark" ]; then
stop-crail.sh && start-crail.sh
sleep 5
fi

CORES=14
NODES=2
EXECUTOR_PER_NODE=$(( EXECUTORS / $NODES))
CORES=$(( CORES / (EXECUTOR_PER_NODE+1) + 1 ))
echo $CORES


#!/bin/bash
# Queries with join ops: 28, 40, 49, 51, 61, 72, 77, 78, 80, 88, 90, 93, 97
#for i in 72 # 4 16 64 95 
#do
#hdfs dfs -rm -R /tpcds-output

start_time="$(date -u +%s)"

if [ "$SYSTEM" == "Spark" ]; then
hdfs dfs -rm -R /tpcds-output-spark
./bin/spark-submit -v \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM --driver-memory 8g --master yarn \
	--conf "spark.app.name=tpcds-$QUERY" \
	--conf "spark.driver.extraClassPath=/home/jyeo/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:." \
	--conf "spark.executor.extraClassPath=/home/jyeo/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:." \
    --conf "spark.sql.adaptive.enabled=$AQE" \
    --conf "spark.sql.autoBroadcastJoinThreshold=$BJ_SIZE" \
	--class com.ibm.crail.benchmarks.Main \
	~/sql-benchmarks/target/sql-benchmarks-1.0.jar \
	-t $QUERY -i /tpcds-$SF -a save,/tpcds-output-spark/ -of parquet \
	2>&1 | tee tpcds_log 
else
./bin/spark-submit -v \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM --driver-memory 8g --master yarn \
	--conf "spark.app.name=tpcds-$QUERY" \
	--conf "spark.driver.extraClassPath=/home/jyeo/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.executor.extraClassPath=/home/jyeo/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.disagg.evictpolicy=DRDD" \
	--conf "spark.disagg.autocaching=true" \
	--conf "spark.disagg.threshold=$DISAGG" \
	--conf "spark.disagg.dagpath=$DAG_PATH" \
	--class com.ibm.crail.benchmarks.Main \
	~/sql-benchmarks/target/sql-benchmarks-1.0.jar \
	-t $QUERY -i /tpcds-$SF -a save,/tpcds-output-jy/ -of parquet \
	2>&1 | tee tpcds_log 
fi
#done

DATE=`date +"%m-%d"`

echo "DATE $DATE"
DIR=logs/$DATE/$SYSTEM-tpcds-$QUERY-mem$MEM-core$CORES-executors$EXECUTORS-aqe$AQE-broadcast$BJ_SIZE

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
  DIR=logs/$DATE/$SYSTEM-tpcds-$QUERY-mem$MEM-core$CORES-executors$EXECUTORS-$NUM-aqe$AQE-broadcast$BJ_SIZE
done

end_time="$(date -u +%s)"
elapsed="$(($end_time-$start_time))"



mkdir -p $DIR

mv tpcds_log $DIR/log.txt

APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
echo "App ID... $APP_ID"

~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt

message=$message"SYSTEM $SYSTEM\n"
message=$message"Input /tpcds-$SF\n"
message=$message"TPC-DS $QUERY\n"
message=$message"AQE $AQE\n"
message=$message"Broadcast size $BJ_SIZE\n"
message=$message"App ID $APP_ID\n"
message=$message"Executor memory $MEM\n"
message=$message"Executor cores $CORES\n"
message=$message"Executors $EXECUTORS\n"
message=$message"Time $elapsed seconds\n"
message=$message"$DIR\n"

~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt

./send_slack.sh  $message


