#!/bin/bash

NUM=1
DIR=logs/0301/DC-MML-mem40g-$NUM

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
  DIR=logs/0301/DC-MML-mem40g-$NUM
done

mkdir -p $DIR
touch $DIR/log.txt
touch $DIR/time_mem_use.txt

./bin/spark-submit -v \
--packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1 \
--num-executors 5 --executor-cores 8 --executor-memory 40g \
--master yarn --class DecisionTreeRegressorExample \
--conf "spark.driver.memory=4g" \
--conf "spark.driver.cores=6" \
--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
/home/jyeo/experiments_nemo/spark_apps/target/scala-2.11/sparkeval_2.11-1.0.jar \
avazu-app \
2>&1 | tee $DIR/log.txt

# extract app id
APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt

# mem size
cat $DIR/log.txt | grep "Total size" | python3 ~/spark/vanilla_memory_use_parser.py > $DIR/time_mem_use.txt

# mem size per executors
cat $DIR/log.txt | grep "BlockManagerw" | python3 ~/spark/vanilla_perexecutor_parser.py


