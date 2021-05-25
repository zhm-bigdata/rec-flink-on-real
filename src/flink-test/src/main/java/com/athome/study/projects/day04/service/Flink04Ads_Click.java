package com.athome.study.projects.day04.service;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.DefaultRowFormatBuilder;
import org.apache.flink.util.Collector;

// province --> ads clicks
public class Flink04Ads_Click {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        env.readTextFile("input/AdClickLog.csv") // 本地测试
//        env.readTextFile("/opt/data/flink/AdClickLog.csv")// linux 本地
            .process(new ProcessFunction<String, Tuple2<String, Integer>>() { // filter 和 map 的功能
                @Override
                public void processElement(String value, Context ctx,
                    Collector<Tuple2<String, Integer>> out)
                    throws Exception {
                    String[] fields = value.split(",");
                    // 过滤合法数据,长度和时间戳，时间戳判断暂用true（都是同一天的数据）
                    if (fields != null && fields.length == 5 && true) {
                        long adId = Long.parseLong(fields[1]);
                        String province = fields[2];
                        Tuple2<String, Integer> oneClick = Tuple2.of(adId + ":" + province, 1);
                        out.collect(oneClick);
                    }
                }
            })
            .keyBy(ele -> ele.f0)
            .sum(1)
            .addSink(StreamingFileSink.forRowFormat(new Path("/rec/flink/zhanghoumin/"), new SimpleStringEncoder()).build());

        try {
            env.execute("Flink04Ads_Click");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
