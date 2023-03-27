TEST_NAME=LR
EXECUTORS=4
MEM_SIZE=12g

DAG_PATH=None
SAMPLING_TIMEOUT=60
ITER=None

JAR=/home/wonook/HiBench/sparkbench/assembly/target/sparkbench-assembly-8.0-SNAPSHOT-dist.jar

if [ "$TEST_NAME" == "KMeans" ]; then

CLASS=com.intel.hibench.sparkbench.ml.DenseKMeans
ARGS="-k 10 --numIterations 5 --storageLevel MEMORY_ONLY --initMode Random /HiBench/Kmeans/Input/samples"

elif [ "$TEST_NAME" == "LR" ]; then

CLASS=com.intel.hibench.sparkbench.ml.LogisticRegression
ARGS="/HiBench/LR/Input"

elif [ "$TEST_NAME" == "RF" ]; then

CLASS=com.intel.hibench.sparkbench.ml.RandomForestClassification
ARGS="--numTrees 100 --numClasses 2 --featureSubsetStrategy auto --impurity gini --maxDepth 4 --maxBins 32 /HiBench/RF/Input"

elif [ "$TEST_NAME" == "GMM" ]; then

CLASS=com.intel.hibench.sparkbench.ml.GaussianMixtureModel
ARGS="-k 10 --numIterations 5 --storageLevel MEMORY_ONLY /HiBench/GMM/Input/samples"

elif [ "$TEST_NAME" == "SVM" ]; then

CLASS=com.intel.hibench.sparkbench.ml.SVMWithSGDExample
ARGS="--numIterations 100 --storageLevel MEMORY_ONLY --stepSize 1.0 --regParam 0.01 /HiBench/SVM/Input"

else

echo "Undefined application $TEST_NAME"

fi

hdfs dfs -rm -R /HiBench/$TEST_NAME/Output


'''
if [ "$TEST_TYPE" != "Spark" ]; then
./run_atc21.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $DAG_PATH $CLASS $JAR $AUTOUNPERSIST $SAMPLING_TIMEOUT $ARGS
else
./run_spark.sh $MEM_SIZE $EXECUTORS $CLASS $JAR $ARGS
fi
'''

AUTOUNPERSIST=false

./run_atc21.sh $TEST_NAME LRU $MEM_SIZE $EXECUTORS $DAG_PATH $CLASS $JAR $AUTOUNPERSIST $SAMPLING_TIMEOUT $ITER $ARGS
#./run_atc21.sh $TEST_NAME LFU $MEM_SIZE $EXECUTORS $DAG_PATH $CLASS $JAR $AUTOUNPERSIST $SAMPLING_TIMEOUT $ITER $ARGS
./run_atc21.sh $TEST_NAME LRC $MEM_SIZE $EXECUTORS $DAG_PATH $CLASS $JAR $AUTOUNPERSIST $SAMPLING_TIMEOUT $ITER $ARGS
./run_atc21.sh $TEST_NAME MRD $MEM_SIZE $EXECUTORS $DAG_PATH $CLASS $JAR $AUTOUNPERSIST $SAMPLING_TIMEOUT $ITER $ARGS
./run_atc21.sh $TEST_NAME GD $MEM_SIZE $EXECUTORS $DAG_PATH $CLASS $JAR $AUTOUNPERSIST $SAMPLING_TIMEOUT $ITER $ARGS
