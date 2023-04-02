#TEST_TYPE=Spark
#TEST_TYPE=Blaze-Mem-Disk
#TEST_TYPE=Blaze-Stage-Ref
EXECUTORS=4

FRACTION=0.25
MEM_FRACTION=0.25
DAG_PATH=None

CLASS=com.ibm.crail.benchmarks.Main
JAR=/home/ubuntu/blaze_benchmarks/sql-benchmarks/target/sql-benchmarks-1.0.jar
MEM_OVERHEAD=4g

SLACK=0
PROMOTE=0.0
PROFILE_TIMEOUT=60

MEM_SIZE=35g
DISK_THRESHOLD=80000g
#TEST_TYPE=Spark

TEST_NAME=PR
TEST_TYPE=LRC
DISAGG=80000g

#ARGS="-t pagerank -gi 10 -i /8m-vertex-graph.txt" 
#ARGS="-t pagerank -gi 10 -i /8m-small-vertex-graph.txt" 
ARGS="-t pagerank -gi 10 -i /pr-too-small" 

TEST_TYPE=Blaze-Disk-Recomp-Cost
#./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $DISK_THRESHOLD $ARGS 


CLASS=org.apache.spark.examples.graphx.ConnectedComponentsExample
JAR=/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar
TEST_NAME=CC
#ARGS="/8m-small-vertex-graph.txt"
ARGS="/pr-too-small"

./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $DISK_THRESHOLD $ARGS 

#./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $DISK_THRESHOLD $ARGS 

#TEST_TYPE=Blaze-Disk-Cost
#./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $DISK_THRESHOLD $ARGS 

