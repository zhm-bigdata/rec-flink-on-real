package com.athome.study.test;

import java.io.IOException;
import java.util.Set;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class RedisClusterTest {

    public static void main(String[] args) {

        HostAndPort hostAndPort = new HostAndPort("hadoop100", 6379);
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setTestOnBorrow(true);
        JedisCluster jedisCluster = new JedisCluster((Set<HostAndPort>) hostAndPort, poolConfig);
        String test = jedisCluster.get("test");

        jedisCluster.set("test","string0");

        try {
            jedisCluster.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
