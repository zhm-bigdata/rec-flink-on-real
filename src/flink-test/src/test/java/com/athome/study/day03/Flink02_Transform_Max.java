package com.athome.study.day03;

import com.athome.study.common.MySourceSensor;
import com.athome.study.common.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_Transform_Max {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(3);
        env
            .addSource(new MySourceSensor())
            .returns(WaterSensor.class)
            .keyBy(new KeySelector<WaterSensor, String>() {
                @Override
                public String getKey(WaterSensor value) throws Exception {
                    return value.getId();
                }
            })
            // max, maxBy区别
            .maxBy("vc",false)

            .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
