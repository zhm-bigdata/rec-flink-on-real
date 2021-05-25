package com.athome.study.projects.day04.service;

import com.athome.study.projects.day04.bean.UserBehavior;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink02UV {

    // 考虑数据倾斜，采用 多并行度，多slot处理重复数据，使用hashmap去重，只发送统计时间段内的同一用户的第一个数据，并定时清除内存种已过时的hashmap数据------实际生产可以考虑redis，既可以定时清除，又可以做数据高可用（服务宕机后重启，redis会有保存的数据）
    public static void main(String[] args) {
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
                Date date = new Date(tiemstamp);
                DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
                String dateFormatted = format.format(date);
                return Tuple3.of(userId, dateFormatted, 1L);
            }).setParallelism(4)
            .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.LONG))
            .keyBy(ele -> ele.f0 + ele.f1)
            .process(
                new KeyedProcessFunction<String, Tuple3<Long, String, Long>, Tuple3<Long, String, Long>>() {
                    private Set<String> hashSet = new HashSet();

                    @Override
                    public void processElement(Tuple3<Long, String, Long> value, Context ctx,
                        Collector<Tuple3<Long, String, Long>> out) throws Exception {
                        String key = value.f0 + value.f1;
                        boolean contains = hashSet.contains(key);
                        if (!contains) {
                            hashSet.add(key);
                            out.collect(value);
                        }
                    }
                }).setParallelism(4)
            .keyBy(ele -> ele.f1)
            .sum(2).setParallelism(4)
            .print();

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
            .map(ele -> {
                String[] fields = ele.split(",");
                Long userId = Long.parseLong(fields[0]);
                Long itemId = Long.parseLong(fields[1]);
                Integer categoryId = Integer.parseInt(fields[2]);
                String behavior = fields[3];
                Long tiemstamp = Long.parseLong(fields[4]);

                UserBehavior userBehavior = new UserBehavior(userId, itemId, categoryId, behavior,
                    tiemstamp);
                return userBehavior;
            })
            .keyBy(behaviro -> behaviro.getUserId())
            .process(new KeyedProcessFunction<Long, UserBehavior, Tuple2<Long, Long>>() {
                private HashMap<Long, Long> map = new HashMap<>();

                @Override
                public void processElement(UserBehavior value, Context ctx,
                    Collector<Tuple2<Long, Long>> out) throws Exception {
                    Long userId = value.getUserId();
                    Long number = map.getOrDefault(userId, 0L);
                    number++;
                    map.put(userId, number);

                }
            });

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
