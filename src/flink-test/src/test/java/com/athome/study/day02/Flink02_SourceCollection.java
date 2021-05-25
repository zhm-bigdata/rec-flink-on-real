package com.athome.study.day02;

import com.athome.study.common.WaterSensor;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_SourceCollection {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<WaterSensor> waterSensors = Arrays.asList(
            new WaterSensor("ws_001", 1577844001L, 45),
            new WaterSensor("ws_002", 1577844015L, 43),
            new WaterSensor("ws_003", 1577844020L, 42));

        env
            .fromCollection(waterSensors)
            .print();

        try {
            env.execute("Collection");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
