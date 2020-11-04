kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092 \
--replication-factor 3 \
--partitions 3
kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
kafka-console-producer.sh --topic left --broker-list localhost:9092
kafka-console-producer.sh --topic right --broker-list localhost:9092
kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092



kafka-topics.sh --create --topic left --bootstrap-server localhost:9092
kafka-topics.sh --create --topic right --bootstrap-server localhost:9092
left 9223372036854775808
right 9223372036854775808
left 0
