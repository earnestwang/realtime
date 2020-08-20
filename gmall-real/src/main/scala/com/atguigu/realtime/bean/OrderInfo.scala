package com.atguigu.realtime.bean

import java.util.Date

case class OrderInfo(id: String,
                     province_id: String,
                     var consignee: String,
                     order_comment: String,
                     var consignee_tel: String,
                     order_status: String,
                     payment_way: String,
                     user_id: String,
                     img_url: String,
                     total_amount: Double,
                     expire_time: String,
                     delivery_address: String,
                     create_time: String,
                     operate_time: String,
                     tracking_no: String,
                     parent_order_id: String,
                     out_trade_no: String,
                     trade_body: String,
                     var create_date: String = null,
                     var create_hour: String = null) {
  //2020-08-19 17:09:45
  create_date = create_time.substring(0, 10)
  create_hour = create_time.substring(11,13)
}
