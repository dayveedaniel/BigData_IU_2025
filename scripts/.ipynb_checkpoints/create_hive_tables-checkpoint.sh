hdfs dfs -test -d project/warehouse/avsc \
  && hdfs dfs -rm -r -skipTrash project/warehouse/avsc
hdfs dfs -mkdir -p project/warehouse/avsc

# use avro file format with bzip2 compression , import schema from output folder
hdfs dfs -put output/avro_bzip2/src/*.avsc project/warehouse/avsc
password=$(head -n 1 secrets/.psql.pass)
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team5 -p $password -f sql/db.hql > output/hive_results.txt