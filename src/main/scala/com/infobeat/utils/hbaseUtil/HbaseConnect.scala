package com.infobeat.utils.hbaseUtil

import java.io.{File, FileInputStream, InputStream}
import java.util.Properties

import com.infobeat.utils.Read_Filel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.slf4j.LoggerFactory

object HbaseConnect {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private var conf: Configuration = _
  private var conn: Connection = _
  private val hbasePro = Read_Filel.getPro("hbase.properties")

  /**
   * 初始化habse连接
   */
  def init(): Unit = {
    if (conf == null) {
      conf = HBaseConfiguration.create
      conf.set("hbase.zookeeper.property.clientPort",hbasePro.getProperty("hbase.zk.port"))
      conf.set("hbase.zookeeper.quorum",hbasePro.getProperty("hbase.zk.quorum"))
      conf.set("hbase.master",hbasePro.getProperty("hbase.master"))
    }
    try {
      conn = ConnectionFactory.createConnection(conf)
    } catch {
      case e: Exception =>
        LOGGER.error(s"创建Hbase连接失败 ", e.printStackTrace())

    }
  }

  /**
   * 获取hbase 连接
   *
   * @return
   */
  def getHbaseConn(): Connection = {
    if (conn == null) init()
    conn
  }

  def main(args: Array[String]): Unit = {
    val table = getHbaseConn().getTable(TableName.valueOf("test"))
    val get = new Get("001|56D6CE9002573BE5".getBytes())
    val result = table.get(get)
    for (kv <- result.rawCells()) {
      println(new String(kv.getQualifier))
      println(new String(kv.getValue))
    }


  }
}
