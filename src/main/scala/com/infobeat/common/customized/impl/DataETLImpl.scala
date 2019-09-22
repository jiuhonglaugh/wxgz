package com.infobeat.common.customized.impl

import java.util

import org.json.JSONObject


abstract class DataETLImpl[T] {
  def dataETL(appkeyMap: util.Map[String, String], logJson: JSONObject, rowkList: List[String]): T

  def dataETL(appkeyMap: util.Map[String, String], logJson: JSONObject, rowk: String): T
}
