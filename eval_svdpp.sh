TEST_NAME=SVDPP
TEST_TYPE=Blaze-Disk-Recomp-Cost
EXECUTORS=4

FRACTION=0.5
MEM_FRACTION=0.2
DAG_PATH=None

CLASS=org.apache.spark.examples.graphx.SVDPlusPlusExample
JAR=/home/ubuntu/blaze/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.4.jar
ARGS=/svdpp-tiny
MEM_OVERHEAD=3g

SLACK=0
PROMOTE=0.0
PROFILE_TIMEOUT=60

MEM_SIZE=25g
DISAGG=100g
./drdd_runner_atc21.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $DISAGG $ARGS 



















