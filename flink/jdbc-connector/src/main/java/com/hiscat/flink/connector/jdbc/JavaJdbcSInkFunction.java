package com.hiscat.flink.connector.jdbc;

import com.mysql.jdbc.Driver;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JavaJdbcSInkFunction extends RichSinkFunction<RowData> {
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open");
        try {
            System.out.println(Thread.currentThread().getContextClassLoader());
            System.out.println(getClass().getClassLoader());
            Class.forName(Driver.class.getName());
            Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306?useSSL=false", "root", "000000");
            System.out.println(connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(RowData value, Context context) throws Exception {
        System.out.println("invoke");
    }

    @Override
    public void close() throws Exception {
        System.out.println("close");
    }
}
