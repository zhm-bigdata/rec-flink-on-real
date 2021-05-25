package com.athome.study.day03;

import com.athome.study.common.MySourceSensor;
import com.athome.study.common.MySourceString;
import com.athome.study.common.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_Transform_Process {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator process = env
            .addSource(new MySourceString())
            .returns(String.class)
            .process(new ProcessFunction<String, WaterSensor>() {
                @Override
                public void processElement(String value, Context ctx, Collector<WaterSensor> out)
                    throws Exception {
                    String[] split = value.split(",");

                    if (split == null || split.length != 3) {

                    } else {
                        String id = split[0].trim();
                        String time = split[1].trim();
                        long ts = Long.parseLong(time.substring(0, time.length() - 1));
                        String watermark = split[2].trim();
                        Integer vc = Integer.getInteger(watermark);
                        WaterSensor waterSensor = new WaterSensor(id, ts, vc);
                        out.collect(waterSensor);
                    }
                }
            });
        process.rescale()
            .print();



        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
