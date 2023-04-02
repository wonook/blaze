#!/bin/bash

./bin/spark-submit -v \
--num-executors 5 --executor-cores 4 --executor-memory 32G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.ALSExample \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar \
data/mllib/als/sample_output \
2>&1 | tee log_als

#APP_ID=`cat log_als | grep -oP "application_[0-9]*_[0-9]*" | tail -n 1`
#~/get_log.sh $APP_ID
#mv log_$APP_ID log_als_yarn
#cat log_als_yarn | grep 'job-wide'
