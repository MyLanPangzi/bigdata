./bin/flink run -p 2 \
-c com.hiscat.flink.scala.SocketStreamWordCount \
/opt/module/flink/test/flink-wordcount-1.0-SNAPSHOT.jar \
--host localhost --port 7777

./bin/flink cancel 455a67af059aa06931c6e859b91f64e7