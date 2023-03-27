OUTPUT=$1
NUMUSER=$2
NUMDATAPERUSER=$3

./bin/spark-submit -v \
--num-executors 8 --executor-cores 16 --executor-memory 50G --driver-memory 10G \
--master yarn --class DataGenALS \
/home/wonook/experiments_nemo/spark_apps/target/scala-2.11/spark-eval_2.11-1.0.jar \
/$OUTPUT $NUMUSER $NUMDATAPERUSER

