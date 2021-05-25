package com.athome.study.day02;

import java.util.Arrays;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FLink04_SourceSocket {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> sourceSockert = env.socketTextStream("hadoop100", 9999);

        sourceSockert
            .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                @Override
                public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
                    String[] words = value.split(" ");
                    Arrays.stream(words).map(ele-> Tuple2.of(ele,1)).forEach(out::collect);
                }
            })
            .keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
                @Override
                public String getKey(Tuple2<String,Integer> value) throws Exception {
                    return value.f0;
                }
            })
            .sum(1)
            .filter(new FilterFunction<Tuple2<String, Integer>>() {
                @Override
                public boolean filter(Tuple2<String, Integer> value) throws Exception {
                    return value.f1==1;
                }
            })
            .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
