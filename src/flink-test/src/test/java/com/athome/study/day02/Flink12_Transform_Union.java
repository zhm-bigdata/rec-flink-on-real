package com.athome.study.day02;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink12_Transform_Union {

    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> firstSource = env.socketTextStream("hadoop100", 9999);
        DataStreamSource<String> secondSource = env.socketTextStream("hadoop101", 9999);

        DataStream<String> union = firstSource
            .union(secondSource);
        union.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
