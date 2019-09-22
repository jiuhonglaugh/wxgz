package com.infobeat.common.customized

import java.util

import com.infobeat.common.customized.impl.DataETLImpl
import com.infobeat.utils.{DateUtils, JSONUtil}
import com.infobeat.utils.hbaseUtil.HbaseUtil.getNeedFileds
import com.infobeat.utils.hbaseUtil.impl.HbaseClientimpl
import com.infobeat.utils.hbaseUtil.{HbaseClient, HbaseUtil}
import org.json.JSONObject


class DefultDataETL extends DataETLImpl[String] {
  val hbaseClient: HbaseClientimpl = new HbaseClient

  override def dataETL(appkeyMap: util.Map[String, String], logJson: JSONObject, rowkList: List[String]): String = {
    var result = ""
    val time = DateUtils.changeUrlTime2Date(logJson.getString("urlTime"))
    val timeStr = DateUtils.getSimpleDate(time)
    logJson.put("time", timeStr)
    val appkey = logJson.get("appKey").toString
    val deviceId = logJson.get("deviceId").toString
    val appkey_rowKey = appkeyMap.getOrDefault(appkey, "") + "|" + deviceId
    val rowKey = HbaseUtil.getRowKeyOfData(deviceId, appkey_rowKey)
    val hbaseMap: util.HashMap[String, String] = HbaseUtil.getHbaseData(rowkList)
    if (hbaseMap.containsKey(rowKey)) {

      val hbaseJson = new JSONObject(hbaseMap.get(rowKey))

      for (field <- getNeedFileds) {
        if (!logJson.has(field) && hbaseJson.has(field)) {
          logJson.put(field, hbaseJson.get(field).toString)
        }
      }
      result = logJson.toString
    }
    result
  }

  override def dataETL(appkeyMap: util.Map[String, String], logJson: JSONObject, rowk: String): String = {
    val time = DateUtils.changeUrlTime2Date(logJson.getString("urlTime"))
    val timeStr = DateUtils.getSimpleDate(time)
    logJson.put("time", timeStr)
    val logMap = JSONUtil.json2Map(logJson)
    val appkey = logJson.get("appKey").toString
    val necessaryFields = getNeedFileds
    val logSet = logMap.keySet
    val set = necessaryFields -- logSet

    val deviceId = logJson.get("deviceId").toString
    val appkey_rowKey = appkeyMap.getOrDefault(appkey, "") + "|" + deviceId

    val rowKey = HbaseUtil.getRowKeyOfData(deviceId, appkey_rowKey)
    val hbaseJson: JSONObject = HbaseUtil.filterForPublicFields(rowKey)
    addNecessaryField(logJson, hbaseJson, set).toString()
  }

  def addNecessaryField(logJson: JSONObject, publicJson: JSONObject, necessaryFields: scala.collection.immutable.Set[String]): JSONObject = {
    val logMap = JSONUtil.json2Map(logJson)
    necessaryFields.foreach { x =>
      if (!logMap.keySet.contains(x) && publicJson.has(x)) {
        logJson.put(x, publicJson.getString(x))
      }
    }
    logJson
  }
}
