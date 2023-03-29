#!/bin/bash

NUM=1
DIR=logs/0301/VECT-mem40g-$NUM

while [ -d "$DIR" ]
do
  # Control will enter here if $DIRECTORY exists.
  NUM=$(( NUM + 1 ))
  DIR=logs/0301/VECT-mem40g-$NUM
done

mkdir -p $DIR
touch $DIR/log.txt
touch $DIR/time_mem_use.txt

./bin/spark-submit -v \
--num-executors 5 --executor-cores 8 --executor-memory 40g \
--master yarn --class org.apache.spark.examples.ml.VectorIndexerExample \
--conf "spark.driver.memory=4g" \
--conf "spark.driver.cores=6" \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar \
kdd12part00 2 \
2>&1 | tee $DIR/log.txt

# extract app id
APP_ID=`cat $DIR/log.txt | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
~/get_log.sh $APP_ID
mv log_$APP_ID $DIR/executor_logs.txt

# mem size
cat $DIR/log.txt | grep "Total size" | python3 ~/spark/vanilla_memory_use_parser.py > $DIR/time_mem_use.txt

# mem size per executors
cat $DIR/log.txt | grep "BlockManagerw" | python3 ~/spark/vanilla_perexecutor_parser.py


