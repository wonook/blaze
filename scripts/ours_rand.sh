#!/bin/bash

APPNAME=$1
APPCLASS=$2 
APPJAR=$3
ARGS=$4
PACKAGES=$5
EXECUTORS=$6
EXECUTOR_MEM_SIZE=$7 # 24g
DISAGG_MEM_SIZE=$8
FRACTION=$9
MEMORY_MANAGER=${10} # Unified / Static / Disagg (all)
STORING_POLICY=${11} # Default (storing evicted blocks into disagg) / No (do not store)
CACHING_POLICY=${12} # None (no caching after getting blocks from disagg) / Local (caching)
AUTOCACHING=${13}
EVICT_POLICY=${14}
DAG_PATH=${15}

# Restart Crail

parallel-ssh -h ~/compute-hosts.txt 'echo splab_wonook | sudo -S rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/compute-hosts.txt 'echo splab_wonook | sudo -S rm -rf /dev/hugepages/data/*'

parallel-ssh -h ~/compute-hosts.txt 'echo splab_wonook | sudo -S rm -rf /disagg/ssd0/yarn/*'

stop-crail.sh && start-crail.sh
sleep 5

# Create log files

NUM=1
DIR=logs/0225/$APPNAME-evict$EVICT_POLICY-autocaching$AUTOCACHING-mem$EXECUTOR_MEM_SIZE-disaggmem$DISAGG_MEM_SIZE-$NUM

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
  DIR=logs/0225/$APPNAME-evict$EVICT_POLICY-autocaching$AUTOCACHING-mem$EXECUTOR_MEM_SIZE-disaggmem$DISAGG_MEM_SIZE-$NUM
done

mkdir -p $DIR
touch $DIR/log.txt

# Spark configs

./bin/spark-submit -v \
--num-executors $EXECUTORS --executor-cores 8 --executor-memory $EXECUTOR_MEM_SIZE \
--master yarn --class $APPCLASS \
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
--conf "spark.disagg.threshold=$DISAGG_MEM_SIZE" \
--conf "spark.storage.memoryFraction=$FRACTION" \
--conf "spark.disagg.dagpath=$DAG_PATH" \
--conf "spark.disagg.evictpolicy=$EVICT_POLICY" \
--conf "spark.disagg.autocaching=$AUTOCACHING" \
$APPJAR \
4 10 true mnist8m \
2>&1 | tee $DIR/log.txt 

touch  $DIR/disagg_usage.txt
du -sh /dev/hugepages/ > $DIR/disagg_usage.txt


# extract app id
APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt

# disagg size 
cat $DIR/log.txt |  grep -oP "Disagg total size: [0-9]* \(MB\)" > $DIR/disagg_mem.txt

# mem size
cat $DIR/log.txt | grep "Total size" | python3 memory_use_parser.py > $DIR/time_mem_use.txt

# mem size per executors
cd $DIR
cat $DIR/log.txt | grep "BlockManagerw" | python3 ~/spark/perexecutor_parser.py
