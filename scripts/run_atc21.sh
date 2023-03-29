#!/bin/bash

# user args
TEST_NAME=$1 # ALSExample
TEST_TYPE=$2
MEM_SIZE=$3 # 24g
EXECUTORS=$4
EXECUTOR_DISK_CAPACITY_MB=$5 
DAG_PATH=$6 # None: no dag path, we need sampling
CLASS=$7 # PR: com.ibm.crail.benchmarks.Main
JAR=$8
PROMOTION_RATIO=${9}
SAMPLING_TIMEOUT=${10}
ARGS="${@:11}"

branch=`git rev-parse --abbrev-ref HEAD`

echo "current branch $branch"
echo "test type $TEST_TYPE"

parallel-ssh -i -h /home/ubuntu/compute-hosts.txt 'rm -rf /home/ubuntu/blaze_cache/*'	

echo "Remove spark shuffle dir"
parallel-ssh -i -h /home/ubuntu/compute-hosts.txt 'rm -rf /home/ubuntu/blaze_shuffle/*'	


# system args
SAMPLING=true
CACHING_POLICY=Blaze
MEMORY_MANAGER=Unified
AUTOCACHING=true

# Deprecated?
DISK_LOCALITY_UNAWARE=false
DISABLE_LOCAL_CACHING=false 


if [[ $TEST_TYPE == "Spark" ]]; then
AUTOCACHING=false
SAMPLING=false

elif [ "$TEST_TYPE" == "Blaze-Memory-Disk" ]; then

COST_FUNCTION=Blaze-Memory-Disk
EVICT_POLICY=Cost-based

elif [ "$TEST_TYPE" == "Blaze-Memory-Only" ]; then

COST_FUNCTION=Blaze-Memory-Only
EVICT_POLICY=Cost-based

elif [ "$TEST_TYPE" == "LRC" ]; then

COST_FUNCTION=LRC
EVICT_POLICY=Cost-based
AUTOCACHING=false
SAMPLING=false
DAG_PATH=None

elif [ "$TEST_TYPE" == "MRD" ]; then

COST_FUNCTION=MRD
EVICT_POLICY=Cost-based
AUTOCACHING=false
SAMPLING=false
DAG_PATH=None

else

echo "No $TEST_TYPE!!!!!!!!!!!!!1"
exit 125

fi

parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /home/ubuntu/blaze_cache/*'
parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /home/ubuntu/blaze_cache/data/*'

CORES=14
NODES=10
EXECUTOR_PER_NODE=$(( EXECUTORS / $NODES))
CORES=$(( CORES / (EXECUTOR_PER_NODE + EXECUTOR_PER_NODE) + 1 ))
echo $CORES

NAME=a
NUM=1
DATE=`date +"%m-%d"`
echo "DATE $DATE"
timestamp=$(date +%s)

DIR=logs/$DATE/$1/$TEST_TYPE-executors$EXECUTORS-core$CORES-mem$MEM_SIZE-disk$EXECUTOR_DISK_CAPACITY_MB-autocaching$AUTOCACHING-sampling$SAMPLING-cost$COST_FUNCTION-evict$EVICT_POLICY-promote$PROMOTION_RATIO-$timestamp-$NUM

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
DIR=logs/$DATE/$1/$TEST_TYPE-executors$EXECUTORS-core$CORES-mem$MEM_SIZE-disk$EXECUTOR_DISK_CAPACITY_MB-autocaching$AUTOCACHING-sampling$SAMPLING-cost$COST_FUNCTION-evict$EVICT_POLICY-promote$PROMOTION_RATIO-$timestamp-$NUM
done


echo "ARGUMENT $ARGS"
echo "JAR $JAR"
echo "CLASS $CLASS"

rm sampled_log.txt
rm sampled_lineage.txt
rm spark_log.txt
rm completed.txt


sampling_start="$(date -u +%s)"

if [[ $TEST_TYPE != "Spark" ]] && [ "$DAG_PATH" == "None" ] && [ "$SAMPLING" == "true" ]; then
	echo "Sampling!!"
	true

rm blaze.log
rm sampling_done.txt
rm killed.txt 

sampling_killer.sh $SAMPLING_TIMEOUT &

#--conf "spark.driver.extraClassPath=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:/home/ubuntu/hadoop/share/hadoop/common/lib/hadoop-aws-2.7.2.jar:/home/ubuntu/hadoop/share/hadoop/common/lib/aws-java-sdk-1.7.4.jar" \
#EXTRA_PATH=/home/ubuntu/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:/home/ubuntu/hadoop/share/hadoop/common/lib/*:$CRAIL_JAR/*
#--conf "spark.driver.extraClassPath=$EXTRA_PATH" \
#--conf "spark.executor.extraClassPath=$EXTRA_PATH" \

# Sampling
./bin/spark-submit -v \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.blaze.autocaching=false" \
	--conf "spark.blaze.autounpersist=false" \
	--conf "spark.blaze.sampledRun=true" \
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

sleep 5

fi


mv blaze.log sampled_blaze.log
rm blaze.log
rm disk_log.txt

parallel-ssh -i -h $HOME/compute-hosts.txt 'rm -rf /home/ubuntu/blaze_cache/*'	

echo "Remove spark shuffle dir"
parallel-ssh -i -h $HOME/compute-hosts.txt 'rm -rf /home/ubuntu/blaze_shuffle/*'	


echo "Application Starts Running"

echo "Start exception observer and killer.."
python exception_killer.py &


start_time="$(date -u +%s)"
#--packages com.microsoft.ml.spark:mmlspark_2.12:1.0.0-rc1 \

./bin/spark-submit -v \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.yarn.am.memory=4000m" \
	--conf "spark.yarn.am.cores=2" \
	--conf "spark.driver.memory=48000m" \
	--conf "spark.driver.cores=8" \
	--conf "spark.rpc.lookupTimeout=300s" \
	--conf "spark.memory.memoryManager=$MEMORY_MANAGER" \
	--conf "spark.blaze.cachingpolicy=$CACHING_POLICY" \
	--conf "spark.blaze.executorDiskCapacity=$EXECUTOR_DISK_CAPACITY_MB" \
	--conf "spark.blaze.dagpath=$DAG_PATH" \
	--conf "spark.blaze.costfunction=$COST_FUNCTION" \
	--conf "spark.blaze.evictionpolicy=$EVICT_POLICY" \
	--conf "spark.blaze.autocaching=$AUTOCACHING" \
	--conf "spark.blaze.sampledRun=false" \
	--conf "spark.blaze.promotionRatio=$PROMOTION_RATIO" \
	--conf "spark.rpc.netty.dispatcher.numThreads=160" \
	--conf "spark.driver.maxResultSize=2g" \
	$JAR $ARGS \
	2>&1 | tee spark_log.txt



echo "Creating $DIR"
mkdir -p $DIR
mv sampled_lineage.txt $DIR/
mv sampled_log.txt $DIR/
mv sampled_blaze.log $DIR/
mv spark_log.txt $DIR/log.txt

touch completed.txt

end_time="$(date -u +%s)"
elapsed="$(($end_time-$start_time))"
sampling_time="$(($sampling_end-$sampling_start))"


# wait exception killer and disk logger
wait

if [ -f "disk_log.txt" ]; then
mv disk_log.txt $DIR/
fi


# extract app id
APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`

EXCEPTION=`cat $DIR/log.txt | grep Exception | head -3`
CANCEL=`cat $DIR/log.txt | grep cancelled because | head -3`
ABORT=`cat $DIR/log.txt | grep aborted | head -3`

if [ -z "${EXCEPTION// }" ]; then
echo "No exception"
else
	message="Exception happended!!!!! $EXCEPTION\n"
fi

if [ -z "${CANCEL// }" ]; then
echo "No cancellation"
else
	message="Job cancelled!!!!! $CANCEL\n"
fi

if [ -z "${ABORT// }" ]; then
echo "Not aborted"
else
	message="Job aborted!!!!! $ABORT\n"
fi

COMMIT=`git log --pretty=format:'%h' -n 1`

message=$message"git commit $COMMIT\n"
message=$message"$DIR\n"
message=$message"App $APP_ID\n"
message=$message"Local memory (MB) $MEM_SIZE\n"
message=$message"Local disk (MB) $EXECUTOR_DISK_CAPACITY_MB\n"
message=$message"Sampling $sampling_time sec (timeout $SAMPLING_TIMEOUT)\n"
message=$message"JCT $elapsed sec\n"
message=$message"Executors $EXECUTORS\n"
message=$message"Args $ARGS\n"
message=$message"--conf spark.blaze.promotionRatio=$PROMOTION_RATIO\n"
message=$message"--conf spark.blaze.costfunction=$COST_FUNCTION\n"
message=$message"--conf spark.blaze.autocaching=$AUTOCACHING\n"
message=$message"--conf spark.blaze.cachingpolicy=$CACHING_POLICY\n"
message=$message"--conf spark.blaze.evictionpolicy=$EVICT_POLICY\n"

./send_slack.sh  $message

sleep 10

~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt
touch $DIR/commit-$COMMIT
touch $DIR/appid-$APP_ID

# history parsing 
if [[ ${#APP_ID} -gt 5 ]]; then
	hdfs dfs -get /spark_history/$APP_ID $DIR/history.txt
fi

GCTIME=`cat $DIR/total_gc_time.txt`

message=$message"GC time: $GCTIME\n"


mv blaze.log $DIR/

# disk size 
cat $DIR/log.txt |  grep -oP "Disagg total size: [0-9]* \(MB\)" > $DIR/disk_mem.txt

# mem size
cat $DIR/log.txt | grep "Total size" | python3 memory_use_parser.py > $DIR/time_mem_use.txt

# recomp time
cat $DIR/log.txt | grep "RCTime" | python3 recomp_parser.py > $DIR/recomp.txt

# evict tcost
cat $DIR/log.txt | grep "EVICT" | python3 evict_parser.py > $DIR/evict.txt
