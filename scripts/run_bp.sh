./bin/spark-submit -v \
--num-executors 4 --executor-cores 4 --executor-memory 50G --driver-memory 40G \
--master yarn --class org.graphframes.examples.BeliefPropagation \
/home/jyeo/graphframes/target/scala-2.11/graphframes-assembly-0.7.1-SNAPSHOT-spark2.4.jar \
3200 \
2>&1 | tee log_bp
