./bin/spark-submit -v \
--num-executors 4 --executor-cores 4 --executor-memory 50G --driver-memory 10G \
--master yarn --class DataGenWordCount \
/home/ubuntu/experiments_nemo/spark_apps/target/scala-2.12/spark-eval_2.12-1.0.jar

