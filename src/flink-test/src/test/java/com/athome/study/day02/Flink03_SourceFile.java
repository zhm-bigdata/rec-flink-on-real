package com.athome.study.day02;

import com.athome.study.common.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_SourceFile {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/sensor.txt");
        source
            .map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                if (split == null || split.length != 3) {
                    return null;
                }
                String id = split[0].trim();
                String time = split[1].trim();
                long ts = Long.parseLong(time.substring(0,time.length() - 1));
                String watermark = split[2].trim();
                Integer vc = Integer.getInteger(watermark);
                return new WaterSensor(id, ts, vc);
            }
        })
            .print();
        try {
            env.execute("file");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
