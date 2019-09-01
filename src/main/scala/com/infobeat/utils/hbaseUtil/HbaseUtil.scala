package com.infobeat.utils.hbaseUtil


import java.util
import java.util.Properties

import com.infobeat.utils.Read_Filel
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.filter.{CompareFilter, MultipleColumnPrefixFilter, QualifierFilter, SubstringComparator}
import org.apache.hadoop.hbase.util.Bytes
import org.json.JSONObject
import org.slf4j.LoggerFactory

object HbaseUtil {
  private val LOGGER = LoggerFactory.getLogger(this.getClass)
  private val hbaseClient = new HbaseClient
  private val hbasePro: Properties = Read_Filel.getPro("hbase.properties")

  def filterForPublicFields(rowKey: String): JSONObject = {
    val date = new java.util.Date()
    val publicJson = new JSONObject()
    val qualifierFilter: QualifierFilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("_")) //列名不包含过滤器 不包含_
    val result = hbaseClient.getKey(rowKey, hbasePro.get("hbase.table.wxgz").toString, qualifierFilter)
    for (kv <- result.rawCells()) {
      publicJson.put(new String(kv.getQualifier), new String(kv.getValue))
    }
    val time = (new java.util.Date().getTime - date.getTime) / 1000
    LOGGER.warn(s"$rowKey 本次查询耗时：$time")
    publicJson
  }

  def getHbaseData(rowKeyList: List[String]): java.util.HashMap[String, String] = {
    val filter: MultipleColumnPrefixFilter = HbaseUtil.multipleFilter()
    val tableName = hbasePro.getProperty("hbase.table.wxgz")
    val start_get = System.currentTimeMillis()
    val resultArr = hbaseClient.getKeys(rowKeyList, tableName, filter)
    val hbaseMap = new java.util.HashMap[String, String]
    if (resultArr.size > 0) {
      for (result <- resultArr) {
        if (!result.isEmpty) {
          val hbaseJson = new JSONObject()
          for (cell <- result.rawCells()) {
            val rowKey = Bytes.toString(result.getRow)
            val key = Bytes.toString(CellUtil.cloneQualifier(cell))
            val value = Bytes.toString(CellUtil.cloneValue(cell))
            hbaseJson.put(key, value)
            hbaseMap.put(rowKey, hbaseJson.toString)
          }
          val end_get = System.currentTimeMillis()
          LOGGER.warn(s"results大小= + ${hbaseMap.size()}  ,耗时： + ${(end_get - start_get)} 毫秒")
        } else {
          LOGGER.warn(s"根据RowKeyList未查询到指定的数据： ${rowKeyList.toString()} ")
        }
      }
    }

    hbaseMap
  }

  /**
   * 根据设备ID和AppKey以及设定的分区数来生成RowKey
   *
   * @param deviceId      设备ID
   * @param appkey_rowKey AppKey
   * @return
   */
  def getRowKeyOfData(deviceId: String, appkey_rowKey: String): String = {
    val deviceIdHashCode = appkey_rowKey.hashCode
    val regionSpilte = hbasePro.getProperty("hbase.rowKey.build.mod").toInt
    //取正数绝对值
    val abs = Math.abs(deviceIdHashCode)
    //取模
    val getMod = abs % regionSpilte
    val newRowKey = "00" + getMod + "|" + appkey_rowKey
    LOGGER.info(s"生成的 RowKey 为： $newRowKey")
    newRowKey
  }

  /**
   * 获取必备字段并且转为Hbase过滤器
   *
   * @return
   */
  def multipleFilter(): MultipleColumnPrefixFilter = {
    var prefixes: Array[Array[Byte]] = Array[Array[Byte]]()
    getNeedFileds.foreach(filed => {
      import Array.concat
      prefixes = concat(prefixes, Array[Array[Byte]](filed.getBytes()))
    })
    val filter = new MultipleColumnPrefixFilter(prefixes)
    filter
  }

  def getNeedFileds(): Set[String] = {
    hbasePro.getProperty("need_filed").split(",").toSet
  }

  def getHbasePro(): Properties = {
    this.hbasePro
  }

  def getList(filterDS: DataStream[String]): List[String] = {
    import org.apache.flink.api.scala._
    var arr: List[String] = null
    filterDS.map(log => {
      val logJson = new JSONObject(log)
      val appkey = logJson.get("appKey").toString
      val deviceId = logJson.get("deviceId").toString
      arr = arr :+ HbaseUtil.getRowKeyOfData(deviceId, appkey)
    })
    arr
  }
}
