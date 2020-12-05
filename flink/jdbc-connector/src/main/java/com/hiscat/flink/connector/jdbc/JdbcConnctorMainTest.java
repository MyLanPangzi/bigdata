package com.hiscat.flink.connector.jdbc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class JdbcConnctorMainTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);
        DataStreamSource<Tuple2<Integer, Boolean>> input = env.fromElements(Tuple2.of(1, false));
        Table table = tEnv.fromDataStream(input, $("id"), $("status"));
        tEnv.executeSql("\n" +
                "CREATE TABLE first_order_user_status (\n" +
                "  id BIGINT,\n" +
                "  status BOOLEAN,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc-extended',\n" +
                "   'url' = 'jdbc:phoenix:hadoop102',\n" +
                "   'driver' = 'org.apache.phoenix.jdbc.PhoenixDriver',\n" +
                "   'table-name' = 'USER_ORDER_STATUS',\n" +
                "   'enable-upsert' = 'true'\n" +
                ")\n");
        tEnv.toAppendStream(table, Row.class).print("test");
        table.executeInsert("first_order_user_status");
        env.fromElements(1).setParallelism(1).print();
        env.execute("test");
    }
}
