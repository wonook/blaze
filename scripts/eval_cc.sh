#TEST_TYPE=Spark
#TEST_TYPE=Blaze-Mem-Disk
#TEST_TYPE=Blaze-Stage-Ref
EXECUTORS=6

FRACTION=0.4
MEM_FRACTION=0.4
DAG_PATH=None

MEM_OVERHEAD=4g

CLASS=org.apache.spark.examples.graphx.ConnectedComponentsExample
JAR=/home/wonook/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar
ARGS="/8m-small-vertex-graph.txt"

SLACK=0
PROMOTE=0.0
PROFILE_TIMEOUT=60

MEM_SIZE=30g
DISK_THRESHOLD=80000g
#TEST_TYPE=Spark


TEST_NAME=CC
DISAGG=80000g
TEST_TYPE=Spark
#./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $DISK_THRESHOLD $ARGS 

TEST_TYPE=Spark-Autocaching-CC
./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $DISK_THRESHOLD $ARGS 


#TEST_TYPE=Blaze-Disk-Recomp-Cost
#./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $DISK_THRESHOLD $ARGS 

#TEST_TYPE=Blaze-Disk-Cost
#./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $DISK_THRESHOLD $ARGS 

