package com.infobeat.utils.redisUtil.impl

import java.util

abstract class RedisClientImpl {


  def getKey(key: String): String

  def getKeys(keys: String): java.util.Set[String]

  def getHall(key: String): util.Map[String, String]

  def setMap(key: String, map: util.Map[String, String])
}
