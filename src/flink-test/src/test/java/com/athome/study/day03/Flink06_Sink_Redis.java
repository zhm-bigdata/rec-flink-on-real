package com.athome.study.day03;

import com.athome.study.common.MySourceSensor;
import com.athome.study.common.WaterSensor;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FixedDelayRestartStrategyConfiguration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig.Builder;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.producer.ProducerConfig;

public class Flink06_Sink_Redis {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(0, 1000));
        HashSet<InetSocketAddress> nodes = new HashSet<>();
        nodes.add(new InetSocketAddress("hadoop100", 6379));
        nodes.add(new InetSocketAddress("hadoop103", 6379));
        nodes.add(new InetSocketAddress("hadoop104", 6379));
        FlinkJedisClusterConfig clusterConfig = new Builder()
            .setNodes(nodes)
            .setTimeout(2000)
            .setMinIdle(1)
            .setMaxIdle(8)
            .setMaxRedirections(100)
            .build();
        env.addSource(new MySourceSensor())
            .returns(WaterSensor.class)
            .map(new MapFunction<WaterSensor,WaterSensor>() {
                @Override
                public WaterSensor map(WaterSensor value) throws Exception {
                    return value;
                }
            })
            .addSink(new RedisSink(clusterConfig,
                new RedisMapper<WaterSensor>() {
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.HSET, "watersensor");
                    }

                    @Override
                    public String getKeyFromData(WaterSensor o) {
                        return o.getId();
                    }

                    @Override
                    public String getValueFromData(WaterSensor o) {
                        ObjectMapper mapper = new ObjectMapper();
                        String valueAsString = null;
                        try {
                            valueAsString = mapper.writeValueAsString(o);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        return valueAsString;
                    }
                })).name("redisClusterSink");
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
