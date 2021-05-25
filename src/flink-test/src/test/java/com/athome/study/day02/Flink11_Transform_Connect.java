package com.athome.study.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink11_Transform_Connect {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> firstSource = env.socketTextStream("hadoop100", 9999);
        DataStreamSource<String> secondSource = env.socketTextStream("hadoop101", 9999);

        ConnectedStreams<String, String> connectedStreams = firstSource
            .connect(secondSource);
        SingleOutputStreamOperator map = connectedStreams.map(new CoMapFunction<String, String,String>() {
            @Override
            public String map1(String value) throws Exception {
                return value + "----first";
            }

            @Override
            public String map2(String value) throws Exception {
                return value + "----second";
            }
        });
        map.print();
        connectedStreams.getFirstInput().print();
        connectedStreams.getSecondInput().print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
