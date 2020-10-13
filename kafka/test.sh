kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092 \
--replication-factor 3 \
--partitions 3
kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092



