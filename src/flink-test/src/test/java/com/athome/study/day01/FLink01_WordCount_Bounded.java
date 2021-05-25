package com.athome.study.day01;

import java.util.Arrays;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FLink01_WordCount_Bounded {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> textFile = env.readTextFile("input/word.txt");

        textFile.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                throws Exception {
                Arrays.stream(value.split(" ")).forEach(
                    ele->{
                        Tuple2<String, Integer> singleTuple2 = Tuple2.of(ele, 1);
                        out.collect(singleTuple2);
                    }
                );
            }
        })
            .keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                @Override
                public Object getKey(Tuple2<String, Integer> value) throws Exception {
                    return value.f0;
                }
            })
            .sum(1)
            .print();
        try {
            env.execute( "FLink01_WordCount_Bounded");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
