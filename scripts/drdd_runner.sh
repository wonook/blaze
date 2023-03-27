#!/bin/bash
#unused var
AUTOSIZING_COMP=0.5

TEST_NAME=$1 # ALSExample
TEST_TYPE=$2
MEM_SIZE=$3 # 24g
EXECUTORS=$4
FRACTION=$5
DISAGG_THRESHOLD=$6 
DAG_PATH=$7 # None: no dag path, we need sampling
CLASS=$8 # PR: com.ibm.crail.benchmarks.Main
JAR=$9
MEM_OVERHEAD=${10}
SLACK=${11}
PROMOTE=${12}
PROFILE_TIMEOUT=${13}
MEM_FRACTION=${14}
DISK_THRESHOLD=${15}

ARGS="${@:16}"

branch=`git rev-parse --abbrev-ref HEAD`

echo "current branch $branch"



PROFILING=true
CACHING_POLICY=Blaze
MEMORY_MANAGER=Unified
AUTOCACHING=true
DISAGG_ONLY=false
DISAGG_FIRST=false

USE_DISK=false
DISABLE_LOCAL=false

IS_SPARK=false

AUTOUNPERSIST=false

if [[ $TEST_TYPE == "Spark" ]]; then
COST_FUNCTION=No
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None
USE_DISK=true
IS_SPARK=true

elif  [[ $TEST_TYPE == "Spark-Autocaching" ]]; then
COST_FUNCTION=Spark-Autocaching
EVICT_POLICY=Cost-based
AUTOCACHING=true
PROFILING=true
DAG_PATH=None
USE_DISK=true
IS_SPARK=true
AUTOUNPERSIST=true

elif  [[ $TEST_TYPE == "Spark-Autocaching-CC" ]]; then
COST_FUNCTION=Spark-Autocaching-CC
EVICT_POLICY=Cost-based
AUTOCACHING=true
PROFILING=true
DAG_PATH=None
USE_DISK=true
IS_SPARK=true
AUTOUNPERSIST=true

elif [ "$TEST_TYPE" == "Blaze-Disk-Recomp-Cost" ]; then

COST_FUNCTION=Blaze-Disk-Recomp
EVICT_POLICY=Cost-based
USE_DISK=true
AUTOUNPERSIST=true

elif [ "$TEST_TYPE" == "Blaze-Disk-Cost" ]; then

COST_FUNCTION=Blaze-Disk-Only
EVICT_POLICY=Cost-based
USE_DISK=true
AUTOUNPERSIST=true

elif [ "$TEST_TYPE" == "Blaze-Recomp-Cost" ]; then

COST_FUNCTION=Blaze-Recomp-Only
EVICT_POLICY=Cost-based
USE_DISK=true
AUTOUNPERSIST=true


elif [ "$TEST_TYPE" == "Blaze-DAG-Aware-Disk" ]; then

COST_FUNCTION=Blaze-DAG-Aware-Disk
EVICT_POLICY=Cost-based
USE_DISK=true
AUTOUNPERSIST=true

elif [ "$TEST_TYPE" == "Blaze-Disk-Only" ]; then

COST_FUNCTION=Blaze-Stage-Ref
EVICT_POLICY=Cost-size-ratio2
USE_DISK=true
DISABLE_LOCAL=true
AUTOUNPERSIST=true

elif [ "$TEST_TYPE" == "Blaze-GS-Only" ]; then

COST_FUNCTION=Blaze-Stage-Ref
EVICT_POLICY=Cost-size-ratio2
DISABLE_LOCAL=true


elif [ "$TEST_TYPE" == "Blaze-Disk" ]; then
COST_FUNCTION=Blaze-Stage-Ref
EVICT_POLICY=Cost-size-ratio2


elif [ "$TEST_TYPE" == "Blaze-Reverse" ]; then

COST_FUNCTION=Blaze-MRD
EVICT_POLICY=Cost-size-ratio2
DISAGG_FIRST=true

elif [ "$TEST_TYPE" == "Blaze-No-Profiling" ]; then
COST_FUNCTION=Blaze-Disk-Recomp
EVICT_POLICY=Cost-based
USE_DISK=true

AUTOCACHING=true
PROFILING=false
DAG_PATH=None

elif [ "$TEST_TYPE" == "Blaze-No-profile" ]; then
COST_FUNCTION=Blaze-Stage-Ref
EVICT_POLICY=Cost-size-ratio2
AUTOCACHING=false
PROFILING=false
DAG_PATH=None

elif [ "$TEST_TYPE" == "Blaze-Linear-Dist" ]; then

COST_FUNCTION=Blaze-Linear-Dist
EVICT_POLICY=Cost-size-ratio2
AUTOCACHING=true
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-Time-Only" ]; then

COST_FUNCTION=Blaze-Time-Only
EVICT_POLICY=Cost-based
AUTOCACHING=true
PROFILING=true


elif [ "$TEST_TYPE" == "Blaze-Ref-Only" ]; then

COST_FUNCTION=Blaze-Ref-Only
EVICT_POLICY=Cost-based
AUTOCACHING=true
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-Leaf-Cnt" ]; then

COST_FUNCTION=Blaze-Leaf-Cnt
EVICT_POLICY=Cost-based
AUTOCACHING=true
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-Ref-Cnt-RDD" ]; then

COST_FUNCTION=Blaze-Ref-Cnt
EVICT_POLICY=RDD-Ordering
AUTOCACHING=true
PROFILING=true


elif [ "$TEST_TYPE" == "Blaze-Cost-based" ]; then
COST_FUNCTION=Blaze-MRD
EVICT_POLICY=Cost-based

elif [ "$TEST_TYPE" == "Blaze-Explicit" ]; then

COST_FUNCTION=Blaze-MRD
EVICT_POLICY=Cost-size-ratio2
AUTOCACHING=false

elif [ "$TEST_TYPE" == "LRC" ]; then

COST_FUNCTION=LRC
EVICT_POLICY=Cost-based

AUTOCACHING=false
PROFILING=false
DAG_PATH=None
USE_DISK=true

elif [ "$TEST_TYPE" == "Real-LRC-profile" ]; then

COST_FUNCTION=LRC
EVICT_POLICY=Cost-based

AUTOCACHING=false
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-MRD-profile" ]; then

COST_FUNCTION=MRD
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=true

elif [ "$TEST_TYPE" == "Blaze-MRD" ]; then

COST_FUNCTION=MRD
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None

elif [ "$TEST_TYPE" == "MRD" ]; then

COST_FUNCTION=MRD
EVICT_POLICY=Cost-based
AUTOCACHING=false
PROFILING=false
DAG_PATH=None
USE_DISK=true

else

echo "No $TEST_TYPE!!!!!!!!!!!!!1"
exit 125

fi

#MEMORY_MANAGER=$2 # Unified / Static / Disagg (all)
#STORING_POLICY=$3 # Default (storing evicted blocks into disagg) / No (do not store)
#CACHING_POLICY=$4 # None (no caching after getting blocks from disagg) / Local (caching)
#EVICT_POLICY=${10}
#AUTOCACHING=${11}
#AUTOSIZING_COMP=${12}

parallel-ssh -h ~/crail-hosts.txt 'sudo rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/crail-hosts.txt 'sudo rm -rf /dev/hugepages/data/*'

parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /dev/hugepages/data/*' 

parallel-ssh -h ~/compute-hosts.txt 'rm -rf /home/wonook/spark_cache/*'
parallel-ssh -h ~/compute-hosts.txt 'rm -rf /home/wonook/spark_cache/data/*'

stop-crail.sh

echo "Stopped crail..."

#parallel-ssh -h ~/compute-hosts.txt 'sudo rm -rf /home/wonook/xvdc/yarn/*'


# recompile crail 

if [[ $TEST_TYPE == "Blaze-Stage-Ref" ]]; then
echo "Starting crail..."
start-crail.sh
sleep 5
fi


CORES=14
NODES=2
EXECUTOR_PER_NODE=$(( EXECUTORS / $NODES))
CORES=$(( CORES / (EXECUTOR_PER_NODE+EXECUTOR_PER_NODE) + 1 ))
echo $CORES

NAME=a
NUM=1
DATE=`date +"%m-%d"`

echo "DATE $DATE"
DIR=logs/$DATE/$1/$TEST_TYPE-mem$MEM_SIZE-disk$DISK_THRESHOLD-executors$EXECUTORS-fraction$FRACTION-memfrac-$MEM_FRACTION-disagg$DISAGG_THRESHOLD-autocaching$AUTOCACHING-profiling$PROFILING-cost$COST_FUNCTION-evict$EVICT_POLICY-slack$SLACK-mOverhead-$MEM_OVERHEAD-promote$PROMOTE-$NUM

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
DIR=logs/$DATE/$1/$TEST_TYPE-mem$MEM_SIZE-disk$DISK_THRESHOLD-executors$EXECUTORS-fraction$FRACTION-memfrac-$MEM_FRACTION-disagg$DISAGG_THRESHOLD-autocaching$AUTOCACHING-profiling$PROFILING-cost$COST_FUNCTION-evict$EVICT_POLICY-slack$SLACK-mOverhead-$MEM_OVERHEAD-promote$PROMOTE-$NUM

done


echo "ARGUMENT $ARGS"
echo "JAR $JAR"
echo "CLASS $CLASS"


rm sampled_log.txt
rm sampled_lineage.txt
rm spark_log.txt
rm completed.txt


sampling_start="$(date -u +%s)"
FULLY=false

if [ "$DAG_PATH" == "None" ] && [ "$PROFILING" == "true" ]; then
	echo "Sampling!!"
	true

rm blaze.log
rm sampling_done.txt
rm killed.txt 

SAMPLING_TIME=$PROFILE_TIMEOUT
./sampling_killer.sh $SAMPLING_TIME &



if [[ $TEST_NAME == *"LGR"* ]]; then

./bin/spark-submit -v \
--packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1 \
--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
--master yarn --class $CLASS \
--conf "spark.yarn.am.memory=4000m" \
--conf "spark.yarn.am.cores=2" \
--conf "spark.driver.extraClassPath=/home/wonook/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
--conf "spark.driver.memory=32000m" \
--conf "spark.driver.cores=6" \
--conf "spark.executor.extraClassPath=/home/wonook/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
--conf "spark.rpc.lookupTimeout=300s" \
--conf "spark.memory.memoryManager=Unified" \
--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
--conf "spark.storage.memoryFraction=0.6" \
--conf "spark.disagg.costfunction=No" \
--conf "spark.disagg.dagpath=$DAG_PATH" \
--conf "spark.disagg.autocaching=false" \
--conf "spark.disagg.autounpersist=false" \
--conf "spark.disagg.sampledRun=true" \
--conf "spark.driver.maxResultSize=2g" \
$JAR $ARGS \
2>&1 | tee sampled_log.txt
else

./bin/spark-submit -v \
--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
--master yarn --class $CLASS \
--conf "spark.yarn.am.memory=4000m" \
--conf "spark.yarn.am.cores=2" \
--conf "spark.driver.extraClassPath=/home/wonook/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
--conf "spark.driver.memory=32000m" \
--conf "spark.driver.cores=6" \
--conf "spark.executor.extraClassPath=/home/wonook/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
--conf "spark.rpc.lookupTimeout=300s" \
--conf "spark.memory.memoryManager=Unified" \
--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
--conf "spark.storage.memoryFraction=0.6" \
--conf "spark.disagg.dagpath=$DAG_PATH" \
--conf "spark.disagg.costfunction=No" \
--conf "spark.disagg.autocaching=false" \
--conf "spark.disagg.autounpersist=false" \
--conf "spark.disagg.sampledRun=true" \
$JAR $ARGS \
2>&1 | tee sampled_log.txt
fi

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

echo "Waiting.."
wait
echo "Wait done"


DAG_PATH=sampled_lineage.txt
#rm $DIR/sampled_log.txt

sleep 2

if [ -f "killed.txt" ]; then
FULLY=false
else
FULLY=true
fi

fi

mv blaze.log sampled_blaze.log
rm blaze.log
rm disk_log.txt



echo "FULLY $FULLY"

echo "Actual Execution!!"

if [[ $TEST_TYPE != *"Spark"* ]]; then
echo "Start exception observer and killer.."
python3 exception_killer.py &
fi

if [[ $TEST_TYPE == *"Disk"* ]]; then
echo "Start disk usage  observer .."
disk_logger.sh &
fi


start_time="$(date -u +%s)"

echo "start iostat"
nohup /home/wonook/run_iostat.sh > /dev/null 2>&1 &

echo "start iostat anna4"
ssh anna-04 "nohup ./run_iostat.sh > /dev/null 2>&1 &"

if [[ $TEST_NAME == *"LGR"* ]]; then
./bin/spark-submit -v \
	--packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1 \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.yarn.am.memory=4000m" \
	--conf "spark.yarn.am.cores=2" \
	--conf "spark.driver.extraClassPath=/home/wonook/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.driver.memory=48000m" \
	--conf "spark.driver.cores=8" \
	--conf "spark.executor.extraClassPath=/home/wonook/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.rpc.lookupTimeout=300s" \
	--conf "spark.memory.memoryManager=$MEMORY_MANAGER" \
	--conf "spark.caching.dir=/home/wonook/spark_cache" \
    --conf "spark.disagg.cachingpolicy=$CACHING_POLICY" \
	--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.memory.fraction=$MEM_FRACTION" \
	--conf "spark.memory.storageFraction=$FRACTION" \
	--conf "spark.disagg.dagpath=$DAG_PATH" \
	--conf "spark.disagg.disableLocalCaching=$DISABLE_LOCAL" \
	--conf "spark.disagg.costfunction=$COST_FUNCTION" \
	--conf "spark.disagg.evictionpolicy=$EVICT_POLICY" \
	--conf "spark.disagg.autocaching=$AUTOCACHING" \
    --conf "spark.disagg.autounpersist=$AUTOUNPERSIST" \
	--conf "spark.disagg.sampledRun=false" \
	--conf "spark.disagg.memoryslack=$SLACK" \
	--conf "spark.disagg.first=$DISAGG_FIRST" \
	--conf "spark.disagg.promote=$PROMOTE" \
	--conf "spark.disagg.fullyProfiled=$FULLY" \
	--conf "spark.executor.memoryOverhead=$MEM_OVERHEAD" \
	--conf "spark.rpc.netty.dispatcher.numThreads=160" \
	--conf "spark.driver.maxResultSize=2g" \
	--conf "spark.storage.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.disagg.useLocalDisk=$USE_DISK" \
    --conf "spark.disagg.isspark=$IS_SPARK" \
	--conf "spark.disagg.disk.threshold=$DISK_THRESHOLD" \
	$JAR $ARGS \
	2>&1 | tee spark_log.txt

#--conf "spark.memory.fraction=$FRACTION" \
else
./bin/spark-submit -v \
	--num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
	--master yarn --class $CLASS \
	--conf "spark.yarn.am.memory=4000m" \
	--conf "spark.yarn.am.cores=2" \
	--conf "spark.driver.extraClassPath=/home/wonook/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.driver.memory=50000m" \
	--conf "spark.driver.cores=8" \
	--conf "spark.executor.extraClassPath=/home/wonook/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$CRAIL_JAR/*:." \
	--conf "spark.rpc.lookupTimeout=300s" \
	--conf "spark.memory.memoryManager=$MEMORY_MANAGER" \
	--conf "spark.caching.dir=/home/wonook/spark_cache" \
    --conf "spark.disagg.cachingpolicy=$CACHING_POLICY" \
	--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.memory.fraction=$MEM_FRACTION" \
	--conf "spark.memory.storageFraction=$FRACTION" \
	--conf "spark.disagg.dagpath=$DAG_PATH" \
	--conf "spark.disagg.disableLocalCaching=$DISABLE_LOCAL" \
	--conf "spark.disagg.costfunction=$COST_FUNCTION" \
	--conf "spark.disagg.evictionpolicy=$EVICT_POLICY" \
	--conf "spark.disagg.autocaching=$AUTOCACHING" \
    --conf "spark.disagg.autounpersist=$AUTOUNPERSIST" \
	--conf "spark.disagg.sampledRun=false" \
	--conf "spark.disagg.memoryslack=$SLACK" \
    --conf "spark.disagg.isspark=$IS_SPARK" \
	--conf "spark.disagg.first=$DISAGG_FIRST" \
	--conf "spark.disagg.fullyProfiled=$FULLY" \
	--conf "spark.disagg.promote=$PROMOTE" \
	--conf "spark.executor.memoryOverhead=$MEM_OVERHEAD" \
	--conf "spark.rpc.netty.dispatcher.numThreads=160" \
	--conf "spark.storage.threshold=$DISAGG_THRESHOLD" \
	--conf "spark.disagg.useLocalDisk=$USE_DISK" \
	--conf "spark.disagg.disk.threshold=$DISK_THRESHOLD" \
	$JAR $ARGS \
	2>&1 | tee spark_log.txt
fi

echo "Creating $DIR"
mkdir -p $DIR

echo "stop iostat"
ssh anna-04 "pkill iostat"
pkill iostat

cp anna3-iostat_log.txt $DIR/
python3 disk_time_range.py anna3-iostat_log.txt > $DIR/anna3-disk.txt
scp anna-04:/home/wonook/anna4-iostat_log.txt $DIR/
python3 disk_time_range.py $DIR/anna4-iostat_log.txt > $DIR/anna4-disk.txt

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
echo "haha"
else
	message="Exception happended!!!!! $EXCEPTION\n"
fi

if [ -z "${CANCEL// }" ]; then
echo "hoho"
else
	message="Job cancelled!!!!! $CANCEL\n"
fi

if [ -z "${ABORT// }" ]; then
echo "hihi"
else
	message="Job aborted!!!!! $ABORT\n"

fi


COMMIT=`git log --pretty=format:'%h' -n 1`

message=$message"Commit $COMMIT\n"
message=$message"Branch $branch\n"
message=$message"App $APP_ID\n"
message=$message"Local memory $MEM_SIZE\n"
message=$message"Profiling timeout $PROFILE_TIMEOUT\n"
message=$message"Disk $DISK_THRESHOLD\n"
message=$message"Disagg memory $DISAGG_THRESHOLD\n"
message=$message"Time $elapsed seconds\n"
message=$message"Sampling $sampling_time seconds\n"
message=$message"Executors $EXECUTORS\n"
message=$message"Log $DIR\n"
message=$message"Args $ARGS\n"
message=$message"--conf spark.disagg.promote=$PROMOTE\n"
message=$message"--conf spark.memory.memoryManager=$MEMORY_MANAGER\n"
message=$message"--conf spark.disagg.cachingpolicy=$CACHING_POLICY\n"
message=$message"--conf spark.disagg.threshold=$DISAGG_THRESHOLD\n"
message=$message"--conf spark.disagg.costfunction=$COST_FUNCTION\n"
message=$message"--conf spark.disagg.evictionpolicy=$EVICT_POLICY\n"
message=$message"--conf spark.disagg.autocaching=$AUTOCACHING\n"

./send_slack.sh  $message

sleep 20

~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt

# history parsing 
hdfs dfs -get /spark_history/$APP_ID $DIR/history.txt

GCTIME=`cat $DIR/total_gc_time.txt`

message=$message"GC time: $GCTIME\n"


mv blaze.log $DIR/



# disagg size 
cat $DIR/log.txt |  grep -oP "Disagg total size: [0-9]* \(MB\)" > $DIR/disagg_mem.txt


# mem size
cat $DIR/log.txt | grep "Total size" | python3 memory_use_parser.py > $DIR/time_mem_use.txt

# recomp time
cat $DIR/log.txt | grep "RCTime" | python3 recomp_parser.py > $DIR/recomp.txt

# evict tcost
cat $DIR/log.txt | grep "EVICT" | python3 evict_parser.py > $DIR/evict.txt
