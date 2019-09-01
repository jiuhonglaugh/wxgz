package com.infobeat.common.customized

import java.util

import com.infobeat.common.customized.impl.DataETLImpl
import com.infobeat.utils.DateUtils
import com.infobeat.utils.hbaseUtil.HbaseUtil.getNeedFileds
import com.infobeat.utils.hbaseUtil.{HbaseClient, HbaseUtil}
import org.json.JSONObject


object DefultDataETL extends DataETLImpl {
  val hbaseClient = new HbaseClient

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
}
