hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/wonook
hdfs dfs -mkdir /user/wonook/data
hdfs dfs -mkdir /user/wonook/data/mllib
hdfs dfs -mkdir /user/wonook/data/mllib/als
hdfs dfs -put $1 /user/wonook/data/mllib/als/output

