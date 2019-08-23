package com.infobeat.utils.redisUtil

import java.util
import com.infobeat.utils.Read_Filel
import org.slf4j.LoggerFactory

object RedisFieldData {
  private val LOGGER = LoggerFactory.getLogger(RedisFieldData.getClass)

  var appkeyMap: util.Map[String, String] = new util.HashMap[String, String]

  val redisPro = Read_Filel.getPro("redis.properties")

  def refeshMap(): Unit = {
    val rc = new RedisClinet(redisPro)
    val map = rc.getHall(redisPro.getProperty("APPKEY_TABLE"))

    LOGGER.warn("总数 {}", map.size)
    appkeyMap = rc.getHall(redisPro.getProperty("APPKEY_TABLE"))
  }

  def main(args: Array[String]): Unit = {
    refeshMap()
  }
}
