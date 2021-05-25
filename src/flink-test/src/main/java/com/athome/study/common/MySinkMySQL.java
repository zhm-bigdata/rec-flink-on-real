package com.athome.study.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MySinkMySQL extends RichSinkFunction<WaterSensor> {

    private static Connection connection;
    PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager
            .getConnection("jdbc:mysql://hadoop101:3306/practice?useSSL=false", "root",
                "aS,j6hwQrP+D");
        preparedStatement = connection
            .prepareStatement("insert into practice.test values (?,?,?) on duplicate key update vc = ?, ts = ?");
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(WaterSensor value, Context context) throws Exception {
        /*
            +-------+-------------+------+-----+---------+-------+
            | Field | Type        | Null | Key | Default | Extra |
            +-------+-------------+------+-----+---------+-------+
            | id    | varchar(20) | NO   | PRI | NULL    |       |
            | vc    | bigint(10)  | NO   |     | NULL    |       |
            | ts    | mediumtext  | NO   |     | NULL    |       |
            +-------+-------------+------+-----+---------+-------+
         */

        String id = value.getId();
        Integer vc = value.getVc();
        Long ts = value.getTs();
        preparedStatement.setNString(1, id);
        preparedStatement.setInt(2, vc);
        preparedStatement.setLong(3, ts);
        preparedStatement.setInt(4, vc);
        preparedStatement.setLong(5, ts);
        preparedStatement.execute();
    }
}
