package com.athome.study.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Flink06_SourceMy {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);




        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    class MySource implements SourceFunction<String>{

      @Override
        public void run(SourceContext<String> ctx) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }

}
