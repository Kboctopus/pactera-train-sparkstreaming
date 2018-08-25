package com.pactera.spark

import com.pactera.java.JedisUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import redis.clients.jedis.Jedis


/**
  * Created by 17427LF on 18/7/6.
  */
object HelloWOrld {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new StreamingContext(sparkConf, Durations.seconds(5));
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "192.168.200.10:9092",
      "group.id" -> "keduox"
    )
    var topics = Set[String]("pactera");
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      sc, kafkaParams, topics)

    stream.map(_._2.split(",")).map { data =>
      ((data(1), data(2)), 1)
    }.reduceByKey(_+_)
        .map { info=>
          (System.currentTimeMillis(), "中国", "四川", "成都", info._2, info._1._1, info._1._2)
        }.foreachRDD(p=>p.foreach(jedisutil))
    sc.start()
    sc.awaitTermination()
    sc.stop()
  }
  def jedisutil(src:(Long,String,String,String,Int,String,String))= {
    val jedis: Jedis = JedisUtil.connectionJedis
    JedisUtil.inputset(jedis, "date", src._1.toString)
    JedisUtil.inputset(jedis, "country", src._2.toString)
    JedisUtil.inputset(jedis, "province", src._3.toString)
    JedisUtil.inputset(jedis, "city", src._4.toString)
    JedisUtil.inputset(jedis, src._6 + src._7.toString, src._5.toString)
    jedis.close();
  }
}
