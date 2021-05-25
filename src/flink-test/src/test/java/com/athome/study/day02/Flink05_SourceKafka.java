package com.athome.study.day02;

import java.util.Properties;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class Flink05_SourceKafka {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
//        env.setParallelism(3);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Flinkk05_SourceKafka");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        env
            .addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(),
            properties))
            .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
