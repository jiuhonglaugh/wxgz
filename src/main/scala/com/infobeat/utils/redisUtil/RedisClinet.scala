package com.infobeat.utils.redisUtil

import java.util

import com.infobeat.utils.redisUtil.impl.RedisClientImpl
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

class RedisClinet extends RedisClientImpl {

  private val LOGGER = LoggerFactory.getLogger(classOf[RedisClinet])

  /**
   * 从redis中获取数据
   */

  override def getKey(key: String): String = {
    var jedis: Jedis = null
    var key = ""
    try {
      jedis = RedisConnect.getJedis
      key = jedis.get(key)
    } catch {
      case e: Exception =>
        LOGGER.warn(s"获取redis缓存： key = $key 失败", e.printStackTrace())
    } finally if (jedis != null) RedisConnect.close(jedis)
    key
  }

  /**
   *
   * @param keys 想要获取的redis-key
   * @return
   */
  override def getKeys(keys: String): util.Set[String] = {
    var jedis: Jedis = null
    var keyes: java.util.Set[String] = null
    try {
      jedis = RedisConnect.getJedis
      keyes = jedis.keys(keys)
    } catch {
      case e: Exception =>
        LOGGER.warn(s"获取redis： keys : $keys 失败", e.printStackTrace())
    } finally if (jedis != null) RedisConnect.close(jedis)
    keyes
  }

  /**
   * 根据 key 获取所有 Hash 数据
   *
   * @param key 想要获取的key
   * @return
   */
  override def getHall(key: String): util.Map[String, String] = {
    var jedis: Jedis = null
    var keyes: util.Map[String, String] = null
    try {
      jedis = RedisConnect.getJedis
      keyes = jedis.hgetAll(key)
    } catch {
      case e: Exception =>
        LOGGER.warn(s"获取redis： key : $key 失败", e.printStackTrace())
    } finally if (jedis != null) RedisConnect.close(jedis)
    keyes
  }

  /**
   * 将 Map 数据 写入对应的Key中
   *
   * @param key redis key
   * @param map 即将写入的数据集
   */
  override def setMap(key: String, map: util.Map[String, String]): Unit = {
    var jedis: Jedis = null
    try {
      jedis = RedisConnect.getJedis
      jedis.hmset(key, map)
      LOGGER.warn(s"redis key : $key 更新成功 数据大小为： ${map.size()}========================")
    } catch {
      case e: Exception =>
        LOGGER.warn(s"redis key : $key 更新失败 数据大小为： ${map.size()}======================", e.printStackTrace())
    } finally if (jedis != null) RedisConnect.close(jedis)
  }
}
