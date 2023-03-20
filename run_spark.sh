CORES=32
NODES=1
EXECUTOR_PER_NODE=$(( EXECUTORS / $NODES))
CORES=$(( CORES / (EXECUTOR_PER_NODE) ))
echo $CORES

./bin/spark-submit -v \
        --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $MEM_SIZE \
        --master yarn --class $CLASS \
        --conf "spark.yarn.am.memory=4g" \
        --conf "spark.yarn.am.cores=2" \
        --conf "spark.driver.memory=4g" \
        --conf "spark.driver.cores=8" \
        --conf "spark.driver.maxResultSize=2g" \
	--conf "spark.rpc.lookupTimeout=300s" \
        --conf "spark.rpc.netty.dispatcher.numThreads=160" \
        $JAR $ARGS \
        2>&1 | tee spark_log.txt
