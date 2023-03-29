./bin/spark-submit -v \
--packages com.microsoft.ml.spark:mmlspark_2.12:1.0.0-rc1 \
--num-executors 6 --executor-cores 4 --executor-memory 40G --driver-memory 1G \
--master yarn --class LRExample \
/home/ubuntu/experiments_nemo/spark_apps/target/scala-2.12/spark-eval_2.12-1.0.jar \
iris_libsvm.txt \
2>&1 | tee log_lr

