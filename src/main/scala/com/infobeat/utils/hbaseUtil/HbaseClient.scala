package com.infobeat.utils.hbaseUtil

import java.util

import com.infobeat.utils.hbaseUtil.impl.HbaseClientimpl
import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.filter.{MultipleColumnPrefixFilter, QualifierFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

class HbaseClient extends HbaseClientimpl {

  private final val LOGGER = LoggerFactory.getLogger(this.getClass)

  /**
   * 根据 某一个rowKey 来查询一个结果
   *
   * @param rowKey
   * @return
   */
  override def getKey(rowKey: String, tableName: String): Result = {
    val table: Table = HbaseConnect.getTable(tableName)
    var result: Result = null
    try {
      result = table.get(new Get(rowKey.getBytes()))
    } catch {
      case e => LOGGER.error("", e.printStackTrace())
    } finally if (table != null) table.close()
    result
  }

  /**
   * 根据 某一个rowKey 并且进行过滤 来查询一个结果
   *
   * @param rowKey    hbaseRowKey
   * @param tableName hbase表名
   * @param qfilter   过滤器
   * @return
   */
  override def getKey(rowKey: String, tableName: String, qfilter: QualifierFilter): Result = {
    val table: Table = HbaseConnect.getTable(tableName)
    val get = new Get(rowKey.getBytes())
    get.setFilter(qfilter)
    var result: Result = null
    try {
      result = table.get(get)
    } catch {
      case e => LOGGER.error("", e.printStackTrace())
    } finally if (table != null) table.close()

    result
  }

  /**
   * 根据一个 rowKey 集合来查询一个结果集
   *
   * @param rowKetList
   * @return
   */
  override def getKeys(rowKetList: List[String], tableName: String): Array[Result] = {
    val table: Table = HbaseConnect.getTable(tableName)
    var resultList: Array[Result] = null
    val getList = new util.ArrayList[Get](rowKetList.size)
    for (rowKey <- rowKetList) {
      getList.add(new Get(Bytes.toBytes(rowKey)))
    }
    try {
      resultList = table.get(getList)
    } catch {
      case e => LOGGER.error("", e.printStackTrace())
    } finally if (table != null) table.close()
    resultList
  }

  /**
   * 根据 某一个rowKey 并且进行过滤 来查询一个结果集
   *
   * @param rowKetList hbaseRowKey
   * @param tableName  hbase表名
   * @param qfilter    过滤器
   * @return
   */
  override def getKeys(rowKetList: List[String], tableName: String, qfilter: QualifierFilter): Array[Result] = {
    val table: Table = HbaseConnect.getTable(tableName)
    var resultList: Array[Result] = null
    val getList = new util.ArrayList[Get](rowKetList.size)
    for (rowKey <- rowKetList) {
      getList.add(new Get(Bytes.toBytes(rowKey)).setFilter(qfilter))
    }
    try {
      resultList = table.get(getList)
    } catch {
      case e => LOGGER.error("", e.printStackTrace())
    } finally if (table != null) table.close()
    resultList

  }

  override def getKeys(rowKetList: List[String], tableName: String, mcfilter: MultipleColumnPrefixFilter): Array[Result] = {
    val table: Table = HbaseConnect.getTable(tableName)
    val getList = new util.ArrayList[Get](rowKetList.size)
    rowKetList.foreach(rowKey => {
      getList.add(new Get(rowKey.getBytes()).setFilter(mcfilter))
    })
    var result: Array[Result] = null
    try {
      result = table.get(getList)
    } catch {
      case e => LOGGER.error("", e.printStackTrace())
    } finally if (table != null) table.close()
    result
  }
}
