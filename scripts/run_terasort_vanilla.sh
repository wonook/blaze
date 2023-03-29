hdfs dfs -rm -R /terasort-output

parallel-ssh -h ./compute-hosts.txt 'echo splab_ubuntu | sudo -S rm -rf /dev/hugepages/cache/*'
parallel-ssh -h ./compute-hosts.txt 'echo splab_ubuntu | sudo -S rm -rf /dev/hugepages/data/*'

parallel-ssh -h ~/compute-hosts.txt 'echo splab_ubuntu | sudo -S rm -rf /disagg/ssd0/yarn/*'

./bin/spark-submit -v \
--num-executors 4 --executor-cores 4 --executor-memory 4G --driver-memory 1G \
--master yarn --class com.ibm.crail.terasort.TeraSort \
/home/ubuntu/crail-spark-terasort/target/crail-spark-terasort-2.0.jar \
-v -i /terasort-input-$1g -o /terasort-output \
2>&1 | tee output

