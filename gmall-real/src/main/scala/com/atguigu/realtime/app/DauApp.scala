package com.atguigu.realtime.app

import java.lang

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.StartupLog
import com.atguigu.realtime.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.util.Constant
import org.apache.phoenix.spark.toProductRDDFunctions
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 1. 从kafka获取启动日志流
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_LOG_TOPIC)
    // 2. 解析启动日志, 每条日志放入一个样例类
    val startupLogStream: DStream[StartupLog] = sourceStream.map(jsonLog => JSON.parseObject(jsonLog, classOf[StartupLog]))
    // 3. 去重, 只保留每个设备的第一次启动记录
    /*val filteredStartedLogStream: DStream[StartupLog] = startupLogStream.filter(log => {
      // 把mid 写到redis, 如果返回的是0 保留, 否则 , 去掉
      val client: Jedis = RedisUtil.getRedisClient
      val key: String = s"mids:${log.logDate}"
      val res: lang.Long = client.sadd(key, log.mid)
      client.close()
      res == 1
    })*/


    // 3. 一个分区建立一个到redis的链接
    val filteredStartupLogStream: DStream[StartupLog] = startupLogStream.mapPartitions(startupLogIt => {
      val client: Jedis = RedisUtil.getRedisClient
      val res: Iterator[StartupLog] = startupLogIt.filter(log => {
        val key = s"mids:${log.logDate}"
        val r: lang.Long = client.sadd(key, log.mid)
        r == 1
      })
      client.close()
      res
    })




    // 4. 把数据写入到Hbase (phoenix)
    import org.apache.phoenix.spark._
    filteredStartupLogStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        tableName = "gmall_dau",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
        zkUrl = Option("hadoop102,hadoop103,hadoop104:2181")
      )
    })


    ssc.start()
    ssc.awaitTermination()


  }

}
