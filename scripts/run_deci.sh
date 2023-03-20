#!/bin/bash

NUM=1
DIR=logs/0301/DC-mem40g-$NUM

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
  DIR=logs/0301/DC-mem40g-$NUM
done

mkdir -p $DIR
touch $DIR/log.txt
touch $DIR/time_mem_use.txt

./bin/spark-submit -v \
--num-executors 5 --executor-cores 8 --executor-memory 40g \
--master yarn --class org.apache.spark.examples.ml.DecisionTreeRegressionExample \
--conf "spark.driver.memory=4g" \
--conf "spark.driver.cores=6" \
/home/jyeo/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar \
2>&1 | tee $DIR/log.txt

# extract app id
APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt


