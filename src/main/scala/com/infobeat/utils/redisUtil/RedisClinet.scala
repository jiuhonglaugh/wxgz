package com.infobeat.utils.redisUtil

import java.util
import java.util.Properties
import com.infobeat.utils.redisUtil.impl.RedisClientImpl
import redis.clients.jedis.Jedis

class RedisClinet extends RedisClientImpl {

  private var pool: BaseRedisConnect = _

  def this(pro: Properties) {
    this
    pool = new BaseRedisConnect(pro)
  }

  /**
   * 从redis中获取数据
   */

  override def getKey(key: String): String = {
    var jedis: Jedis = null
    var key = ""
    try {
      jedis = pool.getJedis
      key = jedis.get(key)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (jedis != null) pool.close(jedis)
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
      jedis = pool.getJedis
      keyes = jedis.keys(keys)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (jedis != null) pool.close(jedis)
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
      jedis = pool.getJedis
      keyes = jedis.hgetAll(key)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (jedis != null) pool.close(jedis)
    keyes
  }
}
