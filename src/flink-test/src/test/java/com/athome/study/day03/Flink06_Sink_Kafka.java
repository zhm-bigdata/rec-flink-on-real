package com.athome.study.day03;

import com.alibaba.fastjson.JSON;
import com.athome.study.common.MySourceSensor;
import com.athome.study.common.WaterSensor;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;


public class Flink06_Sink_Kafka {

    public static void main(String[] args) {

    }

    public static void main1(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(2);

        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        producerConfig.setProperty(ProducerConfig.ACKS_CONFIG,"0");
        env.addSource(new MySourceSensor())
            .returns(WaterSensor.class)
            .map(new MapFunction<WaterSensor,String>() {
                @Override
                public String map(WaterSensor value) throws Exception {
                    ObjectMapper mapper = new ObjectMapper();
                    String valueAsString = mapper.writeValueAsString(value);
                    return valueAsString;
                }
            })
        .addSink(new FlinkKafkaProducer("test_flink_sink",new SimpleStringSchema(),
            producerConfig));
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
