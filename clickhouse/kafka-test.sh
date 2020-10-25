kafka-topics.sh --create --topic ck_test --bootstrap-server localhost:9092 \
--replication-factor 1 \
--partitions 1

kafka-console-consumer.sh --topic ck_test --from-beginning --bootstrap-server localhost:9092
kafka-console-producer.sh --topic ck_test --broker-list localhost:9092
