package com.athome.study.projects.day04.service;

import com.athome.study.projects.day04.bean.UserBehavior;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01PV {


    // 解决数据倾斜?  下面这种做法是错误的；原因： 流式处理，sum求和时重复计算太多太多太多；
    public static void main3(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.readTextFile("input/UserBehavior.csv")
            .filter(ele -> {
                String[] split = ele.split(",");
                if (split == null || split.length < 5) {
                    return false;
                }
                String behavior = split[3];
                if (StringUtils.equalsIgnoreCase(behavior, "pv")) {
                    return true;
                }
                return false;
            })
            .rebalance()
            .map(ele -> Tuple2.of("uv", 1L)).setParallelism(4)
            .returns(Types.TUPLE(Types.STRING, Types.LONG)).setParallelism(4)
            .process(new ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
                private long number;

                @Override
                public void processElement(Tuple2<String, Long> value, Context ctx,
                    Collector<Tuple2<String, Long>> out) throws Exception {
                    number++;
                    out.collect(Tuple2.of(value.f0, number));
                }
            }).setParallelism(4)
            .keyBy(ele -> ele.f0)
            .sum(1)
            .print("pv");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.readTextFile("input/UserBehavior.csv")
            .filter(ele -> {
                String[] split = ele.split(",");
                if (split == null || split.length < 5) {
                    return false;
                }
                String behavior = split[3];
                if (StringUtils.equalsIgnoreCase(behavior, "pv")) {
                    return true;
                }
                return false;
            })
            .map(ele -> Tuple2.of("uv", 1L))
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(ele -> ele.f0)
            .process(
                new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {
                    private long number;

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx,
                        Collector<Tuple2<String, Long>> out) throws Exception {
                        number++;
                        out.collect(Tuple2.of("pv", number));
                    }
                })
            .print("pv");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main1(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment
            .getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.readTextFile("input/UserBehavior.csv")
            .filter(ele -> {
                String[] split = ele.split(",");
                if (split == null || split.length < 5) {
                    return false;
                }
                String behavior = split[3];
                if (StringUtils.equalsIgnoreCase(behavior, "pv")) {
                    return true;
                }
                return false;
            })
            .map(ele -> {
                String[] fields = ele.split(",");
                Long userId = Long.parseLong(fields[0]);
                Long itemId = Long.parseLong(fields[1]);
                Integer categoryId = Integer.parseInt(fields[2]);
                String behavior = fields[3];
                Long tiemstamp = Long.parseLong(fields[4]);
                ;
                UserBehavior userBehavior = new UserBehavior(userId, itemId, categoryId, behavior,
                    tiemstamp);
                return Tuple2.of("uv", 1L);
            })
            .returns(Types.TUPLE(Types.STRING, Types.LONG))
            .keyBy(ele -> ele.f0)
            .sum(1)
            .print("pv");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
