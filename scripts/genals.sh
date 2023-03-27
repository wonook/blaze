./bin/spark-submit -v \
--num-executors 4 --executor-cores 4 --executor-memory 50G --driver-memory 10G \
--master yarn --class DataGenALS \
/home/wonook/experiments_nemo/spark_apps/target/scala-2.11/spark-eval_2.11-1.0.jar

