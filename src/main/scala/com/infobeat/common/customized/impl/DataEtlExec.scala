package com.infobeat.common.customized.impl

import java.util

import org.json.JSONObject

object DataEtlExec {

  def setDataEtl(appkeyMap: util.Map[String, String], bydJson: JSONObject, rowkList: List[String], exeDataEtl: DataETLImpl[String]): String = {
    exeDataEtl.dataETL(appkeyMap: util.Map[String, String], bydJson: JSONObject, rowkList: List[String])
  }

  def setDataEtl(appkeyMap: util.Map[String, String], bydJson: JSONObject, rowkey: String, exeDataEtl: DataETLImpl[String]): String = {
    exeDataEtl.dataETL(appkeyMap: util.Map[String, String], bydJson: JSONObject, rowkey: String)
  }
}
