package com.atguigu.realtime.app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait BaseApp {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("OrderInfoApp")
    val ssc = new StreamingContext(conf, Seconds(3))
    doSomething(ssc)
    // 启动流
    ssc.start()
    // 阻值进程退出
    ssc.awaitTermination()
  }

  def doSomething(ssc : StreamingContext) : Unit

}
