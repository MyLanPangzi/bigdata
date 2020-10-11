./bin/flink run -p 2 \
-c com.hiscat.flink.scala.SocketStreamWordCount \
/opt/module/flink/test/flink-wordcount-1.0-SNAPSHOT.jar \
--host hadoop102 --port 7777

./bin/flink cancel 455a67af059aa06931c6e859b91f64e7
kafka-topics.sh --create --topic flink_source_word_count --bootstrap-server hadoop102:9092
kafka-topics.sh --describe --topic flink_source_word_count --bootstrap-server hadoop102:9092
kafka-console-producer.sh --topic flink_source_word_count --broker-list hadoop102:9092
kafka-console-consumer.sh --topic flink_kafka_sink --from-beginning --bootstrap-server hadoop102:9092
kafka-console-consumer.sh --topic flink_source_word_count --from-beginning --bootstrap-server hadoop102:9092
