package com.infobeat.utils.redisUtil

import java.util
import java.util.Properties

import com.infobeat.utils.Read_Filel
import com.infobeat.utils.redisUtil.impl.RedisClientImpl
import org.slf4j.LoggerFactory

object RedisFieldData {
  private val LOGGER = LoggerFactory.getLogger(RedisFieldData.getClass)
  private var appkeyMap: util.Map[String, String] = _

  val redisPro: Properties = Read_Filel.getPro("redis.properties")
  private val rc: RedisClientImpl = new RedisClinet()

  def getAppkeyMap: util.Map[String, String] = {
    if (appkeyMap == null) {
      val map = rc.getHall(redisPro.getProperty("APPKEY_TABLE"))
      LOGGER.warn("总数 {}", map.size)
      appkeyMap = map
    }
    appkeyMap
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

}
