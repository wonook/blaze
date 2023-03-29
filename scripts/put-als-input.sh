hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/ubuntu
hdfs dfs -mkdir /user/ubuntu/data
hdfs dfs -mkdir /user/ubuntu/data/mllib
hdfs dfs -mkdir /user/ubuntu/data/mllib/als
hdfs dfs -put $1 /user/ubuntu/data/mllib/als/output

