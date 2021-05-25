package com.athome.study.common;

import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.temporal.TemporalField;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySourceSensor implements SourceFunction {

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (true){
//            UUID id = UUID.randomUUID();
            int id = new Random().nextInt(100);
            LocalTime now = LocalTime.now();
            Date date = new Date();
            long ts = date.getTime();
            int vc = new Random().nextInt(40);
            WaterSensor waterSensor = new WaterSensor(id + "id", ts, vc);
            ctx.collect(waterSensor);
            System.out.println(waterSensor);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        //
    }
}
