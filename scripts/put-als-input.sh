hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/jyeo
hdfs dfs -mkdir /user/jyeo/data
hdfs dfs -mkdir /user/jyeo/data/mllib
hdfs dfs -mkdir /user/jyeo/data/mllib/als
hdfs dfs -put $1 /user/jyeo/data/mllib/als/output

