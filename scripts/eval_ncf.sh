TEST_NAME=NCF
TEST_TYPE=LRC
EXECUTORS=4

MEM_SIZE=8g
DISK_SIZE=0g

DAG_PATH=None

PROMOTE=0.0
SAMPLING_TIMEOUT=60

CLASS=com.intel.analytics.zoo.examples.recommendation.NeuralCFexample
JAR=/home/jyeo/analytics-zoo/zoo/target/analytics-zoo-bigdl_0.12.2-spark_2.4.3-0.11.0-SNAPSHOT.jar
ARGS=""

./run_atc21.sh $TEST_NAME $TEST_TYPE $MEM_SIZE $EXECUTORS $DISK_SIZE $DAG_PATH $CLASS $JAR $PROMOTE $SAMPLING_TIMEOUT $ARGS 


