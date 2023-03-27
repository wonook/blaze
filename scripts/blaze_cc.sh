TEST_NAME=CC
EXECUTORS=6

MEM_FRACTION=0.6
FRACTION=0.5
DAG_PATH=None


CLASS=org.apache.spark.examples.graphx.ConnectedComponentsExample
JAR=/home/wonook/spark/examples/target/scala-2.12/jars/spark-examples_2.12-3.0.0.jar
#JAR=/home/wonook/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar
ARGS="/user/wonook/data/graphx/twitter"

MEM_OVERHEAD=4g

SLACK=0
PROMOTE=0.0
TEST_TYPE=Spark
MEM_SIZE=32g
DISAGG=0g
PROFILE_TIMEOUT=60
./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $ARGS

MEM_SIZE=30g
DISAGG=10g
TEST_TYPE=Blaze-Stage-Ref
./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $ARGS

#./shutdown.sh

