package com.infobeat.utils.hbaseUtil

import com.infobeat.utils.Read_Filel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Table}
import org.slf4j.LoggerFactory

object HbaseConnect {

  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private var conf: Configuration = _
  private var conn: Connection = _
  private val hbasePro = HbaseUtil.getHbasePro

  /**
   * 初始化habse连接
   */
  private def init(): Unit = {
    if (conf == null) {
      conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", hbasePro.getProperty("hbase.zk.port"))
      conf.set("hbase.zookeeper.quorum", hbasePro.getProperty("hbase.zk.quorum"))
      conf.set("hbase.master", hbasePro.getProperty("hbase.master"))
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

  /**
   * 根据表名获取一个 table 对象
   *
   * @param tableName 表名称
   * @return
   */
  def getTable(tableName: String): Table = {
    getHbaseConn().getTable(TableName.valueOf(tableName))
  }

  def main(args: Array[String]): Unit = {
    val table = getHbaseConn().getTable(TableName.valueOf("wxgz_scenes_data"))
    val get = new Get("002|AA|774030284007961".getBytes())
    val result = table.get(get)
    for (kv <- result.rawCells()) {
      println(new String(kv.getQualifier))
      println(new String(kv.getValue))
    }


  }
}
