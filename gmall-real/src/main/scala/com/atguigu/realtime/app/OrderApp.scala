package com.atguigu.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.realtime.bean.OrderInfo
import com.atguigu.realtime.util.MyKafkaUtil
import com.atguigu.util.Constant
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object OrderApp extends BaseApp {

  override def doSomething(ssc : StreamingContext) : Unit = {
    val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC)

    val orderInfoStream = sourceStream.map(json => JSON.parseObject(json, classOf[OrderInfo]))

    // 把数据写入到Hbase中
    // orderInfoStream.print()
    import org.apache.phoenix.spark._

    orderInfoStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
"GMALL_ORDER_INFO",
            Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
            zkUrl = Option("hadoop102,hadoop103,hadoop104:2181")
      )

    })





  }
}
