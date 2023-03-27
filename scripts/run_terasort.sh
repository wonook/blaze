hdfs dfs -rm -R /terasort-output
#~/incubator-crail/bin/stop-crail.sh
#~/incubator-crail/bin/start-crail.sh

parallel-ssh -h ./compute-hosts.txt 'echo splab_wonook | sudo -S rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ./compute-hosts.txt 'echo splab_wonook | sudo -S rm -rf /dev/hugepages/data/*'

parallel-ssh -h ~/compute-hosts.txt 'echo splab_wonook | sudo -S rm -rf /disagg/ssd0/yarn/*'

./bin/spark-submit -v \
--num-executors 36 --executor-cores 4 --executor-memory 16G --driver-memory 16G \
--master yarn --class com.ibm.crail.terasort.TeraSort \
--conf "spark.driver.extraClassPath=$CRAIL_JAR/*:$CRAIL_SPARKIO_JAR/crail-spark-1.0.jar:." \
--conf "spark.executor.extraClassPath=$CRAIL_JAR/*:$CRAIL_SPARKIO_JAR/crail-spark-1.0.jar:." \
--conf "spark.shuffle.manager=org.apache.spark.shuffle.crail.CrailShuffleManager" \
--conf "spark.crail.shuffle.sorter=com.ibm.crail.terasort.sorter.CrailShuffleNativeRadixSorter" \
--conf "spark.crail.serializer=com.ibm.crail.terasort.serializer.F22Serializer" \
/home/johnyangk/crail-spark-terasort/target/crail-spark-terasort-2.0.jar \
-v -i /terasort-input-$1g -o /terasort-output \
2>&1 | tee output

