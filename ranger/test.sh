!connect 'jdbc:hive2://hadoop102:10000'
admin
ranger123
show tables;

!connect 'jdbc:hive2://hadoop102:10000'
atguigu
000000
show tables;

hdfs dfs -chmod -R 777 /hive /spark-history /spark-jars /tmp /user
hdfs dfs -chmod -R 777 /