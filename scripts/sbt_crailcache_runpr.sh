#!/bin/bash

TEST_NAME=$1 # ALSExample
MEMORY_MANAGER=$2 # Unified / Static / Disagg (all)
STORING_POLICY=$3 # Default (storing evicted blocks into disagg) / No (do not store)
CACHING_POLICY=$4 # None (no caching after getting blocks from disagg) / Local (caching)
MEM_SIZE=$5 # 24g
EXECUTORS=$6
FRACTION=$7
DISAGG_THRESHOLD=$8
DAG_PATH=$9
EVICT_POLICY=${10}
AUTOCACHING=${11}
ITER=3


parallel-ssh -h ~/compute-hosts.txt 'echo splab_ubuntu | sudo -S rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/compute-hosts.txt 'echo splab_ubuntu | sudo -S rm -rf /dev/hugepages/data/*'

parallel-ssh -h ~/compute-hosts.txt 'echo splab_ubuntu | sudo -S rm -rf /disagg/ssd0/yarn/*'

stop-crail.sh && start-crail.sh

sleep 5

NAME=a
NUM=1
DIR=logs/0206/$1-$NAME-evict$EVICT_POLICY-mem$5-mm$2-sp$3-cp$4-executors$6-fraction$7-disagg$8-iter$ITER-autocaching$AUTOCACHING-$NUM

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
  DIR=logs/0206/$1-$NAME-evict$EVICT_POLICY-mem$5-mm$2-sp$3-cp$4-executors$6-fraction$7-disagg$8-iter$ITER-autocaching$AUTOCACHING-$NUM
done

echo "Creating $DIR"

mkdir -p $DIR

touch $DIR/log.txt


./bin/spark-submit -v \
--num-executors $EXECUTORS --executor-cores 8 --executor-memory $MEM_SIZE \
--master yarn --class com.ibm.crail.benchmarks.Main \
--conf "spark.yarn.am.memory=4000m" \
--conf "spark.yarn.am.cores=2" \
--conf "spark.driver.extraClassPath=$CRAIL_JAR/*:." \
--conf "spark.driver.memory=32000m" \
--conf "spark.driver.cores=6" \
--conf "spark.executor.extraClassPath=$CRAIL_JAR/*:." \
--conf "spark.rpc.lookupTimeout=300s" \
--conf "spark.disagg.storingpolicy=$STORING_POLICY" \
--conf "spark.memory.memoryManager=$MEMORY_MANAGER" \
--conf "spark.disagg.cachingpolicy=$CACHING_POLICY" \
--conf "spark.disagg.threshold=$DISAGG_THRESHOLD" \
--conf "spark.storage.memoryFraction=$FRACTION" \
--conf "spark.disagg.dagpath=$DAG_PATH" \
--conf "spark.disagg.evictpolicy=$EVICT_POLICY" \
--conf "spark.disagg.autocaching=$AUTOCACHING" \
/home/ubuntu/blaze_benchmarks/sql-benchmarks/target/sql-benchmarks-1.0.jar \
-t pagerank -gi $ITER -i /user/ubuntu/twitter \
2>&1 | tee $DIR/log.txt

touch  $DIR/disagg_usage.txt
du -sh /dev/hugepages/ > $DIR/disagg_usage.txt

#--conf "spark.memory.storageFraction=$FRACTION" \

# extract app id
APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt

# disagg size 
cat $DIR/log.txt |  grep -oP "Disagg total size: [0-9]* \(MB\)" > $DIR/disagg_mem.txt


# mem size
cat $DIR/log.txt | grep "Total size" | python3 memory_use_parser.py > $DIR/time_mem_use.txt


