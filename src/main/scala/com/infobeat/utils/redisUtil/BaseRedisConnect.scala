package com.infobeat.utils.redisUtil

import java.util

import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.util.control.Breaks

class BaseRedisConnect {

  private var jedisPool: JedisPool = null
  private val LOGGER = LoggerFactory.getLogger(classOf[BaseRedisConnect])

  /**
   * 初始化redis连接池
   *
   * @param dir redis配置文件地址
   * @return
   */
  def getPool(dir: String): JedisPool = {
    if (jedisPool == null) {
      val config = new JedisPoolConfig
      //控制一个pool可分配多少个jedis实例，通过pool.getJedis()来获取；
      //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
      config.setMaxTotal(50000)
      //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
      config.setMaxIdle(15)
      //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
      config.setMaxWaitMillis(1000 * 100)
      //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
      config.setTestOnBorrow(true)
      val serverip = "172.10.4.87"
      val password = "idyredis87"
      val timeOut = 30000
      val port = 6379
      //      LOGGER.warn("serverip {} ,password {}", serverip, password)
      try {
        jedisPool = new JedisPool(config, serverip, port, timeOut, password)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
      //      LOGGER.warn("连接池 {}", jedisPool.toString)
    }
    jedisPool
  }

  /**
   * 获取连接
   *
   * @param dir
   * @return
   */
  def getJedis(dir: String): Jedis = {
    var resource: Jedis = null
    if (jedisPool != null) {
      resource = jedisPool.getResource
    } else {
      resource = getPool(dir).getResource
    }
    resource
  }

  /**
   * 回收连接
   *
   * @param jedis
   */
  def close(jedis: Jedis) {
    if (jedis != null)
      jedisPool.returnResource(jedis)
  }

  /**
   * 从redis中获取数据
   */

  def get(key: String, dir: String): String = {
    var jedis: Jedis = null
    var value = ""
    try {
      jedis = getJedis(dir)
      val loop = new Breaks;
      loop.breakable {
        while (true) {
          if (null != jedis)
            loop.break()
          else
            jedis = getJedis(dir)
        }
      }
      value = jedis.get(key)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (jedis != null) jedis.close()
    value
  }

  def getKeys(keys: String, dir: String): java.util.Set[String] = {
    var jedis: Jedis = null
    var keyes: java.util.Set[String] = null
    try {
      jedis = getJedis(dir)
      val loop = new Breaks;
      loop.breakable {
        while (true) {
          if (null != jedis)
            loop.break()
          else
            jedis = getJedis(dir)
        }
      }
      keyes = jedis.keys(keys)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally if (jedis != null) jedis.close()
    keyes
  }
}

object a {
  def main(args: Array[String]): Unit = {
    val ba = new BaseRedisConnect
    val jedis = ba.getJedis("")
    val a: util.Map[String, String] = jedis.hgetAll("InterfaceKeyValue")
    ba.close(jedis)
    println(a.toString)
  }
}