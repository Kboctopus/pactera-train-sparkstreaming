package com.pactera.java;

import redis.clients.jedis.Jedis;

/**
 * Created by 17427LF on 18/7/6.
 */
public class JedisUtil {
    public static void main(String[] args) {
        JedisUtil.inputset(JedisUtil.connectionJedis(),"hello","zhangsan");
        System.out.printf(JedisUtil.systemRedis(JedisUtil.connectionJedis(),"hello"));
    }
    /**
     * 往redis添加数据 (key,value)
     * @param key       key
     * @param value     value
     */
    public static void inputset(Jedis jedis,String key,String value){
        jedis.set(key,value);
    }

    /**
     * 连接redis
     * @return
     */
    public static Jedis connectionJedis(){
        Jedis jedis = new Jedis("192.168.200.10",6379);
        return jedis;
    }

    /**
     * 根据KEY查找数据
     * @param key
     * @return
     */
    public static String systemRedis(Jedis jedis,String key){
        return jedis.get(key);
    }
}
