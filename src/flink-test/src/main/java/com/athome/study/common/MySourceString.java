package com.athome.study.common;

import java.time.LocalTime;
import java.util.Date;
import java.util.Random;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MySourceString implements SourceFunction {

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (true) {
//            UUID id = UUID.randomUUID();
            int id = new Random().nextInt(1);
            LocalTime now = LocalTime.now();
            Date date = new Date();
            long ts = date.getTime();
            int vc = new Random().nextInt(40);
            String result = String.format("%s %s %s", id, ts, vc);
            ctx.collect(result);
            System.out.println(result);
            Thread.sleep(300);
        }
    }

    @Override
    public void cancel() {
        //
    }
}
