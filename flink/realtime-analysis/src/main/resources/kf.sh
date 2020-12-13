kafka-topics.sh --create --topic dwd_order_detail \
--bootstrap-server hadoop102:9092 \
--config cleanup.policy=compact \
--partitions 3
kafka-topics.sh --create --topic test_json_2 \
--bootstrap-server hadoop102:9092 \
--partitions 3
kafka-console-producer.sh --broker-list hadoop102:9092 --topic test_json_2
kafka-console-consumer.sh --bootstrap-server hadoop102:9092 --topic test_json_2 --from-beginning
