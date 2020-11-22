kafka-topics.sh --bootstrap-server hadoop102:9092 --create --topic start-topic --partitions 4 --replication-factor 3
kafka-topics.sh --bootstrap-server hadoop102:9092 --create --topic event-topic --partitions 4 --replication-factor 3
kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic start-topic --from-beginning
kafka-console-producer.sh --broker-list hadoop102:9092 --topic start-topic
nohup java -jar log-collect-0.0.1-SNAPSHOT.jar > start.log 2>&1 &