package com.hiscat;

import java.sql.*;
import java.util.Properties;

/**
 * @author hiscat
 */
public class KylinJDBCTest {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();

        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://hadoop102:7070/gmall", info);
        Statement state = conn.createStatement();
        ResultSet rs = state.executeQuery(
                "select region_name,area.name name,gender,sum(TOTAL_AMOUNT) total_amount from dwd_fact_payment_info p \n" +
                        "left join dwd_dim_user_info_view u on p.user_id = u.id\n" +
                        "left join DWD_DIM_BASE_PROVINCE_VIEW area on area.id = p.province_id\n" +
                        "group by region_name,area.name,gender"
        );
        System.out.println("region_name,\t area.name, \t gender, \t total_amount");
        while (rs.next()) {
        System.out.printf("%s,\t %s, \t %s, \t %d\n",rs.getString("region_name"),
                rs.getString("name"),
                rs.getString("gender"),
                rs.getInt("total_amount"));
        }
    }
}
