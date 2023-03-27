spark-submit -v \
--packages com.microsoft.ml.spark:mmlspark_2.11:1.0.0-rc1 \
--num-executors 4 --executor-cores 10 --executor-memory 50G --driver-memory 1G \
--master yarn --class LightGBMRegressorExample \
/home/wonook/experiments_nemo/spark_apps/target/scala-2.11/sparkeval_2.11-1.0.jar \
avazu-app \
2>&1 | tee log_lightgbm


