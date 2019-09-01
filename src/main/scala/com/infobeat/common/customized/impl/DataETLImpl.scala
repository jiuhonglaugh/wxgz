package com.infobeat.common.customized.impl

import java.util

import org.json.JSONObject


abstract class DataETLImpl {
  def dataETL(appkeyMap: util.Map[String, String], logJson: JSONObject, rowkList: List[String]): String
}
