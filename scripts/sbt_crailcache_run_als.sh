#!/bin/bash

parallel-ssh -h ~/compute-hosts.txt 'echo splab_ubuntu | sudo -S rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ~/compute-hosts.txt 'echo splab_ubuntu | sudo -S rm -rf /dev/hugepages/data/*'

parallel-ssh -h ~/compute-hosts.txt 'echo splab_ubuntu | sudo -S rm -rf /disagg/ssd0/yarn/*'

stop-crail.sh && start-crail.sh

sleep 5

./bin/spark-submit -v \
--num-executors 5 --executor-cores 8 --executor-memory 24G --driver-memory 1G \
--master yarn --class org.apache.spark.examples.ml.ALSExample \
--conf "spark.driver.extraClassPath=$CRAIL_JAR/*:$CRAIL_SPARKIO_JAR/crail-spark-1.0.jar:." \
--conf "spark.executor.extraClassPath=$CRAIL_JAR/*:$CRAIL_SPARKIO_JAR/crail-spark-1.0.jar:." \
/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar \
2>&1 | tee log_als_ours

