package com.athome.study.day01;

import java.util.Arrays;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FLink03_WordCount_Unbounded {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env
            .socketTextStream("hadoop100", 9999);

        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                throws Exception {
                String[] split = value.split(" ");
                Arrays.stream(split).forEach(ele ->
                {
                    out.collect(Tuple2.of(ele, 1));
                });
            }
        })
            .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                @Override
                public String getKey(Tuple2<String, Integer> value) throws Exception {
                    return value.f0;
                }
            })
            .sum(1)
            .print();

        try {
            env.execute("FLink03_WordCount_Unbounded");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
