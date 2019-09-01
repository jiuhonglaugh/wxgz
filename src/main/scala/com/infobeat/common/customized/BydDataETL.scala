package com.infobeat.common.customized

import java.util

import com.infobeat.common.customized.impl.DataETLImpl
import org.json.{JSONArray, JSONObject}
import org.slf4j.LoggerFactory


object BydDataETL extends DataETLImpl {
  val LOGGER = LoggerFactory.getLogger(this.getClass.getName)

  override def dataETL(appkeyMap: util.Map[String, String], bydJson: JSONObject, rowkList: List[String]): String = {
    bydJson.put("type", "byd_api_realtime")
    val jsonArr = new JSONArray()
    val jsonObj = new JSONObject()
    try {
      val appkey = bydJson.getString("appKey")
      val deviceId = bydJson.getString("deviceId")
      val ip = bydJson.getString("currentIPAddress")
      val city = bydJson.getString("city")
      val className = bydJson.getString("cN")
      val methedName = bydJson.getString("mN")
      val isp = bydJson.getString("isP")
      val dayTime = bydJson.getString("t")
      val count = bydJson.getString("c")
      val json = new JSONObject()
      json.put("appkey", appkey)
      json.put("deviceId", deviceId)
      json.put("ip", ip)
      json.put("city", city)
      json.put("className", className)
      json.put("methedName", methedName)
      json.put("isp", isp)
      json.put("dayTime", dayTime)
      json.put("count", count)
      jsonArr.put(json)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        LOGGER.error(s"此日志出现问题--比亚迪 ：$bydJson")
    }
    if (jsonArr.length() > 0)
      jsonObj.put("array", jsonArr)

    jsonObj.toString()
  }
}
