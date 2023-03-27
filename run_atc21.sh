#!/bin/bash

######################
### Configs
######################

# user args
TEST_NAME=$1 # ALSExample
METRIC=$2
MEM_SIZE=$3 # 24g
EXECUTORS=$4
DAG_PATH=$5 # None: no dag path, we're sampling
CLASS=$6 # PR: com.ibm.crail.benchmarks.Main
JAR=$7
AUTOUNPERSIST=$8
SAMPLING=$9
SAMPLING_JOBS="${10}"
SAMPLING_TIMEOUT="${11}"
ITER="${12}"
COST="${13}"
ARGS="${@:14}"

HOME_DIR=/home/wonook

echo $SAMPLING_TIMEOUT
echo $ARGS
echo $ITER
echo "Running $METRIC"

echo "Cleanup directories and previous logs"
parallel-ssh -i -h $HOME_DIR/compute-hosts.txt "rm -rf $HOME_DIR/hdfs_dir/caching_dir/*"
parallel-ssh -i -h $HOME_DIR/compute-hosts.txt "rm -rf $HOME_DIR/hadoop/logs/userlogs/*"

df -hT | grep hdfs_dir

rm sampled_log.txt
rm sampled_lineage.txt
rm spark_log.txt
rm blaze.log
rm sampling_done.txt
rm killed.txt 


AUTOCACHING=false
LAZY_AUTOCACHING=false


if [ "$METRIC" == "Blaze-Memory-Disk" ]; then

COST_FUNCTION=Blaze-Memory-Disk
#LAZY_AUTOCACHING=true

elif [ "$METRIC" == "Blaze-Memory-Only" ]; then

COST_FUNCTION=Blaze-Memory-Only

elif [ "$METRIC" == "LRU" ]; then

COST_FUNCTION=LRU

elif [ "$METRIC" == "LFU" ]; then

COST_FUNCTION=LFU

elif [ "$METRIC" == "GD" ]; then

COST_FUNCTION=GD

elif [ "$METRIC" == "LRC" ]; then

COST_FUNCTION=LRC

elif [ "$METRIC" == "MRD" ]; then

COST_FUNCTION=MRD

else

echo "Unsupported $METRIC!"
exit 125

fi

CORES=32
NODES=1
EXECUTOR_PER_NODE=$(( EXECUTORS / $NODES))
CORES=$(( CORES / (EXECUTOR_PER_NODE) ))
echo $CORES


######################
### Create Log Dir
######################


NUM=1
DATE=`date +"%m-%d"`

DIR=logs/$DATE/$1/$METRIC-executors$EXECUTORS-core$CORES-mem$MEM_SIZE-sampling$SAMPLING-sampledJobs$SAMPLING_JOBS-cost$COST-iter$ITER-$NUM

while [ -d "$DIR" ]
do
  # enters here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
  DIR=logs/$DATE/$1/$METRIC-executors$EXECUTORS-core$CORES-mem$MEM_SIZE-sampling$SAMPLING-sampledJobs$SAMPLING_JOBS-cost$COST-iter$ITER-$NUM
done


######################
### Profiling
######################

if [ "$DAG_PATH" == "None" ] && [ "$SAMPLING" == "true" ]; then
	echo "#################### Starting Profiling Run"
	true

sampling_start="$(date -u +%s)"

sampling_killer.sh $SAMPLING_TIMEOUT &

./bin/spark-submit -v \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.blaze.isProfileRun=true" \
	$JAR $ARGS \
	2>&1 | tee sampled_log.txt
touch sampling_done.txt

sampling_end="$(date -u +%s)"

# get dag path 
APP_ID=`cat sampled_log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
echo "Getting sampled lineage... $APP_ID"
sleep 5
hdfs dfs -get /spark_history/$APP_ID sampled_lineage.txt


if [ ! -f "sampled_lineage.txt" ]; then
	hdfs dfs -get /spark_history/"${APP_ID}.inprogress" sampled_lineage.txt
fi

DAG_PATH=sampled_lineage.txt

mv blaze.log sampled_blaze.log

fi



######################
### Actual Run
######################

echo "#################### Starting Actual Run"

echo "Start exception observer and killer.."
python exception_killer.py &

#--conf "spark.driver.extraClassPath=$HOME_DIR/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:$HOME_DIR/hadoop/share/hadoop/common/lib/hadoop-aws-2.7.2.jar:$HOME_DIR/hadoop/share/hadoop/common/lib/aws-java-sdk-1.7.4.jar" \
#--conf "spark.driver.extraClassPath=$EXTRA_PATH" \
#--conf "spark.executor.extraClassPath=$EXTRA_PATH" \
#--packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1 \


start_time="$(date -u +%s)"

./bin/spark-submit -v \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.yarn.am.memory=4g" \
	--conf "spark.yarn.am.cores=2" \
	--conf "spark.driver.memory=4g" \
	--conf "spark.driver.cores=8" \
	--conf "spark.driver.maxResultSize=2g" \
	--conf "spark.rpc.lookupTimeout=300s" \
	--conf "spark.rpc.netty.dispatcher.numThreads=160" \
	--conf "spark.blaze.dagPath=$DAG_PATH" \
	--conf "spark.blaze.autoCaching=$AUTOCACHING" \
	--conf "spark.blaze.isProfileRun=false" \
	--conf "spark.blaze.profileNumJobs=$SAMPLING_JOBS" \
	--conf "spark.blaze.lazyAutoCaching=$LAZY_AUTOCACHING" \
	--conf "spark.blaze.autoUnpersist=$AUTOUNPERSIST" \
	--conf "spark.blaze.costFunction=$COST_FUNCTION" \
	$JAR $ARGS \
	2>&1 | tee spark_log.txt



######################
### Collect Logs
######################

echo "Creating $DIR"
mkdir -p $DIR
mv sampled_lineage.txt $DIR/
mv sampled_log.txt $DIR/
mv sampled_blaze.log $DIR/
mv spark_log.txt $DIR/log.txt


# extract app id
APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`

sleep 5

~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt

# history parsing 
if [[ ${#APP_ID} -gt 5 ]]; then
	hdfs dfs -get /spark_history/$APP_ID $DIR/history.txt
fi

mv blaze.log $DIR/



######################
### Slack Summary
######################

EXCEPTION=`cat $DIR/log.txt | grep Exception | head -3`
CANCEL=`cat $DIR/log.txt | grep cancelled because | head -3`
ABORT=`cat $DIR/log.txt | grep aborted | head -3`

if [ -z "${EXCEPTION// }" ]; then
echo "No exception"
else
	message="Exception happended! $EXCEPTION\n"
fi

if [ -z "${CANCEL// }" ]; then
echo "No cancellation"
else
	message="Job cancelled! $CANCEL\n"
fi

if [ -z "${ABORT// }" ]; then
echo "Not aborted"
else
	message="Job aborted! $ABORT\n"
fi

COMMIT=`git log --pretty=format:'%h' -n 1`
end_time="$(date -u +%s)"
jct="$(($end_time-$start_time))"
sampling_time="$(($sampling_end-$sampling_start))"

message=$message"git commit $COMMIT\n"
message=$message"$DIR\n"
message=$message"App $APP_ID\n"
message=$message"Args $ARGS\n"
message=$message"Profiling $sampling_time sec (timeout $SAMPLING_TIMEOUT)\n"
message=$message"JCT $jct sec\n"

./send_slack.sh  $message

