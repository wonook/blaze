#!/bin/bash

INPUT=$1
NUMUSER=$2
NUMDATAPERUSER=$3

$SPARK_HOME/bin/spark-submit -v \
--num-executors 4 --executor-cores 4 --executor-memory 40G --driver-memory 10G \
--master yarn --class DataGenALS \
/home/ubuntu/experiments_nemo/spark_apps/target/scala-2.12/spark-eval_2.12-1.0.jar \
/$INPUT $NUMUSER $NUMDATAPERUSER

