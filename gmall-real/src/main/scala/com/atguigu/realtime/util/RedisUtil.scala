package com.atguigu.realtime.util

import redis.clients.jedis.Jedis

object RedisUtil {

  private val host = "hadoop102"
  private val port = 6379

  def getRedisClient = new Jedis(host, port);

}
