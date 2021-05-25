package com.athome.study.day01;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FLink01_WordCount_Batch {

    public static void main(String[] args) throws Exception {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            DataSource<String> textFile = env.readTextFile("input");

            FlatMapLine flatMapper = new FlatMapLine();
            AggregateOperator<Tuple2<String, Integer>> result = textFile
                .flatMap(flatMapper)
                .groupBy(0)
                .sum(1);
            result.print();

//            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    static class FlatMapLine implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] array = value.split(" ");
            Arrays.stream(array).forEach(
                ele -> {
                    Tuple2<String, Integer> pair = Tuple2.of(ele, 1);
                    out.collect(pair);
                }
            );
        }
    }

}
