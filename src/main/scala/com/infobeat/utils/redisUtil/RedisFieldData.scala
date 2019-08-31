package com.infobeat.utils.redisUtil

import java.util
import java.util.Properties

import com.infobeat.utils.Read_Filel
import org.slf4j.LoggerFactory

object RedisFieldData {
  private val LOGGER = LoggerFactory.getLogger(RedisFieldData.getClass)
  //  private var userAttributeSet: util.HashSet[String, String] = _
  var appkeyMap: util.Map[String, String] = new util.HashMap[String, String]

  val redisPro: Properties = Read_Filel.getPro("redis.properties")
  val rc = new RedisClinet()

  def refeshMap(): Unit = {
    val map = rc.getHall(redisPro.getProperty("APPKEY_TABLE"))
    LOGGER.warn("总数 {}", map.size)
    appkeyMap = map
  }

  def getRedisSet(his: String): util.HashSet[String] = {
    val tmpSet = new util.HashSet[String]()
    val redisMap = rc.getHall(his).values().toString
    val tmpStr = redisMap.replaceAll("[\\[\"\\] ]", "")
    val tmpArr = tmpStr.split(",")
    for (value <- tmpArr) {
      println(value)
      tmpSet.add(value)
    }
    tmpSet
  }

  def main(args: Array[String]): Unit = {
    getRedisSet(redisPro.getProperty("TERM_USER_ATTRIBUTE"))
    for (i <- 1 to 100) {
      Thread.sleep(2)
      refeshMap()
    }
  }
}
