package com.athome.study.day03;

import com.athome.study.common.MySourceSensor;
import com.athome.study.common.WaterSensor;
import java.time.LocalDateTime;
import java.util.Date;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_SInk_MySQL {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();

        env.setParallelism(1);

        env.addSource(new MySourceSensor())
            .returns(WaterSensor.class)
//            .addSink(new MySinkMySQL());
            .addSink(
                JdbcSink.sink(
                    "insert into practice.test values (?,?,?) on duplicate key update vc = ?, ts = ?"
                    ,(JdbcStatementBuilder<WaterSensor>) (preparedStatement, waterSensor) -> {
                        String id = waterSensor.getId();
                        Integer vc = waterSensor.getVc();
                        Long ts = waterSensor.getTs();

                        ts = Long.valueOf(LocalDateTime.now().getSecond());
                        preparedStatement.setNString(1, id);
                        preparedStatement.setInt(2, vc);
                        preparedStatement.setLong(3, ts);
                        preparedStatement.setInt(4, vc);
                        preparedStatement.setLong(5, ts);
                    },
                     JdbcExecutionOptions.builder()
                    .withBatchSize(10)
                         .withBatchIntervalMs(15000)
                    .withMaxRetries(1)
                    .build()
                    , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withDriverName("com.mysql.jdbc.Driver")
                    .withUrl("jdbc:mysql://hadoop101:3306/practice?useSSL=false")
                    .withUsername("root")
                    .withPassword("aS,j6hwQrP+D")
                    .withConnectionCheckTimeoutSeconds(10).build()
    )).name("Sink_JDBC");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
