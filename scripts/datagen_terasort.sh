./bin/spark-submit -v \
--num-executors 10 --executor-cores 4 --executor-memory 16G \
--master yarn --class com.github.ehiggs.spark.terasort.TeraGen \
--conf "spark.driver.extraClassPath=$CRAIL_JAR/*:$CRAIL_SPARKIO_JAR/crail-spark-1.0.jar:." \
--conf "spark.executor.extraClassPath=$CRAIL_JAR/*:$CRAIL_SPARKIO_JAR/crail-spark-1.0.jar:." \
/home/jyeo/spark-terasort/target/spark-terasort-1.0-jar-with-dependencies.jar \
$1g /terasort-input-$1g
