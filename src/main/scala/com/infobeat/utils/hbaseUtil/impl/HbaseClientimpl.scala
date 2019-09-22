package com.infobeat.utils.hbaseUtil.impl

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.filter.{MultipleColumnPrefixFilter, QualifierFilter}

trait HbaseClientimpl {

  /**
   * 根据 某一个rowKey 来查询一个结果
   *
   * @param rowKey    hbaseRowKey
   * @param tableName hbase表名
   * @return
   */
  def getKey(rowKey: String, tableName: String): Result

  /**
   * 根据 某一个rowKey 并且进行过滤 来查询一个结果
   *
   * @param rowKey    hbaseRowKey
   * @param tableName hbase表名
   * @param qfilter   过滤器
   * @return
   */
  def getKey(rowKey: String, tableName: String, qfilter: QualifierFilter): Result


  /**
   * 根据一个 rowKey 集合来查询一个结果集
   *
   * @param rowKetList hbaseRowKey集合
   * @param tableName  hbase表名
   * @return
   */
  def getKeys(rowKetList: List[String], tableName: String): Array[Result]

  /**
   * 根据 某一个rowKey 并且进行过滤 来查询一个结果集
   *
   * @param rowKetList hbaseRowKey
   * @param tableName  hbase表名
   * @param qfilter    过滤器
   * @return
   */

  def getKeys(rowKetList: List[String], tableName: String, qfilter: QualifierFilter): Array[Result]

  /**
   *
   * @param rowKetList hbaseRowKey
   * @param tableName  hbase表名
   * @param mcfilter   过滤器
   * @return
   */
  def getKeys(rowKetList: List[String], tableName: String, mcfilter: MultipleColumnPrefixFilter): Array[Result]

}
