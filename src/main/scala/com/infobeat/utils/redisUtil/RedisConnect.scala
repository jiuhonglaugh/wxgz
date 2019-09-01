package com.infobeat.utils.redisUtil

import java.lang.reflect.Field
import java.util.Properties

import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisConnect {

  private var jedisPool: JedisPool = _
  private val LOGGER = LoggerFactory.getLogger(RedisConnect.getClass)
  private val pro: Properties = RedisFieldData.redisPro

  private def initPool(): Unit = {
    if (jedisPool == null) {
      LOGGER.warn(s"开始初始化 Redis 连接池")
      val config = new JedisPoolConfig
      LOGGER.warn(s"Redis_Ip: ${
        pro.getProperty("IP")
      }   Pwd： ${
        pro.getProperty("PASSWD")
      } ")
      //控制一个pool可分配多少个jedis实例，通过pool.getJedis()来获取；
      //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
      config.setMaxTotal(pro.getProperty("MAX_TOTA").toInt)
      //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
      config.setMaxIdle(pro.getProperty("MAX_IDLE").toInt)
      //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
      config.setMaxWaitMillis(pro.getProperty("MAX_WAIT_MILLIS").toLong)
      //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
      config.setTestOnBorrow(true)
      try {
        jedisPool = new JedisPool(config,
          pro.getProperty("IP"),
          pro.getProperty("PORT").toInt,
          pro.getProperty("TIME_OUT").toInt,
          pro.getProperty("PASSWD"))
        LOGGER.warn(s"成功初始化 Redis 连接池 ,连接池大小为： ${
          pro.getProperty("MAX_TOTA")
        } ")
      } catch {
        case e: Exception =>
          LOGGER.error("Redis Poll 初始化失败", e.printStackTrace())
      }
    }
  }

  /**
   * 获取连接
   *
   * @return
   */
  def getJedis: Jedis = {
    var conn: Jedis = null
    if (jedisPool != null) {
      conn = jedisPool.getResource
      LOGGER.warn(s"Redis 连接池总大小为： ${pro.getProperty("MAX_TOTA")} 可用连接数为 :${
        pro.getProperty("MAX_TOTA").toInt - jedisPool.getNumActive
      } ")
    } else {
      initPool()
      conn = jedisPool.getResource
      LOGGER.warn(s"Redis 连接池总大小为： ${pro.getProperty("MAX_TOTA")} 可用连接数为 :${
        pro.getProperty("MAX_TOTA").toInt - jedisPool.getNumActive
      } ")
    }
    conn
  }

  /**
   * 回收连接
   *
   * @param jedis redis 连接
   */
  def close(jedis: Jedis) {
    if (jedis != null)
      jedisPool.returnResource(jedis)
  }
}