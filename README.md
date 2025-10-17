docker exec -it namenode bash
hdfs dfs -mkdir -p /user/hdfs/weather
hdfs dfs -ls /user/hdfs
hdfs dfs -chown -R hdfs:supergroup /user/hdfs
hdfs dfs -chmod -R 755 /user/hdfs
hdfs dfs -chmod -R 755 /user/hdfs/weather