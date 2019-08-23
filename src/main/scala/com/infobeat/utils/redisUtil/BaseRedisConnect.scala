package com.infobeat.utils.redisUtil

import java.util
import java.util.Properties

import com.infobeat.utils.Read_Filel
import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.util.control.Breaks

class BaseRedisConnect {

  private var jedisPool: JedisPool = _
  private val LOGGER = LoggerFactory.getLogger(classOf[BaseRedisConnect])
  private var pro: Properties = _

  def this(pro: Properties) {
    this()
    this.pro = pro

  }

  def initPool(): Unit = {
    if (jedisPool == null) {
      val config = new JedisPoolConfig
      //控制一个pool可分配多少个jedis实例，通过pool.getJedis()来获取；
      //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
      config.setMaxTotal(pro.getProperty("MAX_TOTA").toInt)
      //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
      config.setMaxIdle(pro.getProperty("MAX_IDLE").toInt)
      //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
      config.setMaxWaitMillis(pro.getProperty("MAX_WAIT_MILLIS").toLong)
      //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
      config.setTestOnBorrow(true)
      LOGGER.warn(s"Redis_Ip: ${pro.getProperty("IP")}   Pwd： ${pro.getProperty("PASSWD")}")
      try {
        jedisPool = new JedisPool(config,
          pro.getProperty("IP"),
          pro.getProperty("PORT").toInt,
          pro.getProperty("TIME_OUT").toInt,
          pro.getProperty("PASSWD"))
      } catch {
        case e: Exception =>
          LOGGER.error("Redis Poll 初始化失败", e.printStackTrace())
      }
    }
    LOGGER.warn(s"可用连接数： ${jedisPool.getNumActive}")
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
    } else {
      initPool()
      conn = jedisPool.getResource
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

  /**
   * 从redis中获取数据
   */

  def get(key: String): String = {
    var jedis: Jedis = null
    var key = ""
    try {
      jedis = getJedis
      key = jedis.get(key)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (jedis != null) close(jedis)
    key
  }

  /**
   *
   * @param keys 想要获取的redis-key
   * @return
   */
  def getKeys(keys: String): java.util.Set[String] = {
    var jedis: Jedis = null
    var keyes: java.util.Set[String] = null
    try {
      jedis = getJedis
      keyes = jedis.keys(keys)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (jedis != null) jedis.close()
    keyes
  }

  /**
   * 根据 key 获取所有 Hash 数据
   *
   * @param keys  想要获取的key
   * @return
   */
  def getHall(keys: String): util.Map[String, String] = {
    var jedis: Jedis = null
    var keyes: util.Map[String, String] = null
    try {
      jedis = getJedis
      keyes = jedis.hgetAll(keys)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (jedis != null) jedis.close()
    keyes
  }
}