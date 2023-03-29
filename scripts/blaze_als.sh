TEST_NAME=ALSExample
CLASS=org.apache.spark.examples.ml.ALSExample
JAR=/home/ubuntu/blaze/examples/target/scala-2.12/jars/spark-examples_2.12-3.3.2.jar
ARGS="none"
DAG_PATH=None

EXECUTORS=6

MEM_FRACTION=0.6
FRACTION=0.5
DAG_PATH=None

MEM_OVERHEAD=3g

SLACK=0
PROMOTE=0.0
#TEST_TYPE=Blaze-Stage-Ref
TEST_TYPE=Blaze-Stage-Ref
#TEST_TYPE=Blaze-No-profile-autocaching
#TEST_TYPE=Spark
MEM_SIZE=14g
DISAGG=40g
PROFILE_TIMEOUT=60
./drdd_runner.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $FRACTION $DISAGG $DAG_PATH $CLASS $JAR $MEM_OVERHEAD $SLACK $PROMOTE $PROFILE_TIMEOUT $MEM_FRACTION $ARGS

#./shutdown.sh
