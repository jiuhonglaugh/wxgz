package com.infobeat.utils

import java.util

import net.minidev.json.parser.JSONParser
import org.json.JSONObject

import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.mutable

object JSONUtil {
  /**
   * 将map转为json
   * @param map 输入格式 mutable.Map[String,Object]
   * @return
   * */
  def map2Json(map : mutable.Map[String,Object]) : String = {
    val m = new util.HashMap[String,Object]()
    map.foreach(value=>{
      m.put(value._1,value._2)
    })
    val jsonString = net.minidev.json.JSONObject.toJSONString(m)
    jsonString
  }

  def mapToJson(map : mutable.Map[String,String]) : String = {
    val m = new util.HashMap[String,String]()
    map.foreach(value=>{
      m.put(value._1,value._2)
    })
    val jsonString = net.minidev.json.JSONObject.toJSONString(m)
    jsonString
  }

  /**
   * 将json转化为Map
   * @param json 输入json字符串
   * @return
   * */
  def json2Map(json : JSONObject) : mutable.HashMap[String,Object] = {

    val map : mutable.HashMap[String,Object]= mutable.HashMap()
    //获取所有键
    val iterator = json.keySet().iterator()

    while (iterator.hasNext){
      val field = iterator.next()
      val value = if(json.get(field)==null)"" else json.get(field).toString;

      if(value.startsWith("{")&&value.endsWith("}")){
        val str  = if(json.get(field)==null)"" else json.get(field);
        val value = mapAsScalaMap(str.asInstanceOf[util.HashMap[String, String]])
        map.put(field,value)
      }else{
        map.put(field,value)
      }
    }
    map
  }

  /**
   * 将json转化为Map
   *
   * @param json 输入json字符串
   * @return
   **/
  def jsonToMap(json: String): mutable.HashMap[String, Object] = {

    val map: mutable.HashMap[String, Object] = mutable.HashMap()

    val jsonParser = new JSONParser()

    //将string转化为jsonObject
    val jsonObj: net.minidev.json.JSONObject = jsonParser.parse(json).asInstanceOf[net.minidev.json.JSONObject]

    //获取所有键
    val jsonKey = jsonObj.keySet()

    val iter = jsonKey.iterator()

    while (iter.hasNext) {
      val field = iter.next()
      val value = jsonObj.getOrElse(field, null).toString

      if (value.startsWith("{") && value.endsWith("}")) {
        val value = mapAsScalaMap(jsonObj.getOrElse(field, null).asInstanceOf[util.HashMap[String, String]])
        map.put(field, value)
      } else {
        map.put(field, value)
      }
    }
    map
  }

  /**
   * 将json转化为Map
   * 注意：此处所有的key均为小写
   *
   * @param json 输入json字符串
   * @return
   **/
  def json2Map(json: String): Option[mutable.HashMap[String, String]] = {
    try {

      val map: mutable.HashMap[String, String] = mutable.HashMap()
      //      val jsonMap: util.Map[_, _] = JSON.parseObject(json)
      val jsonMap: util.Map[_, _] = toMap(json)

      var matchesError: Boolean = false
      val iterator = jsonMap.keySet.iterator
      while (iterator.hasNext) {
        val key = iterator.next
        val value: String = if (jsonMap.get(key) == None || jsonMap.get(key) == null) null else jsonMap.get(key).toString

        //检查日志是否异常
        if (!matchesError) {
          matchesError = key.toString.matches(".*[ ,;{}()\n\t=].*")
        }
        if (key.toString.matches(".*[ ,;{}()\n\t=].*")) {
          error(("JsonUtils.json2Map one_field exception: ").concat("key为：" + key + "，值为：" + jsonMap.get(key)))
        }
        map.put(TextUtils.filterFieldName(key.toString), value)
      }
      //输出异常的全量日志
      if (matchesError) {
        error("JsonUtils.json2Map all_field exception: ".concat(map.toMap.mkString("\t")))
      }

      Some(map)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }

  }


  def toMap(jsonstr: String): util.Map[String, AnyRef] = {
    try {
      if (jsonstr != null && !"".equals(jsonstr)) {
        val jsonObject = new JSONObject(jsonstr)
        val set: util.Set[String] = jsonObject.keySet
        val iterator: util.Iterator[String] = set.iterator

        //        val iterator: util.Iterator[Map.Entry[String, String]] = endMap.entrySet().iterator()
        val map: util.HashMap[String, AnyRef] = new util.HashMap()
        while (iterator.hasNext) {
          val key: String = iterator.next
          val value: AnyRef = jsonObject.get(key)
          map.put(key, value)
        }
        map
      } else {
        new util.HashMap()
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        new util.HashMap()
    }
  }

  def mapKeyToLowerCase(map: Option[mutable.Map[String, String]]): Option[mutable.Map[String, String]] = {
    try {
      if (map == None || map.get == null || map.get == None) {
        None
      } else {
        val newMap: mutable.Map[String, String] = map.get.map((tuple: (String, String)) => (tuple._1.toLowerCase(), tuple._2))
        Some(newMap)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }

  def toJsonString(map: util.Map[_, _]): String = {
    val jsonObject = new JSONObject(map)
    jsonObject.toString()
  }

  def map2MapOp(map: mutable.Map[String, String]): Option[mutable.Map[String, String]] = {
    val newMap: mutable.HashMap[String, String] = mutable.HashMap()
    try {
      var matchesError: Boolean = false
      for (obj <- map.keySet) {
        //检查日志是否异常
        if (!matchesError) {
          matchesError = obj.toString.matches(".*[ ,;{}()\n\t=].*")
        }
        if (obj.toString.matches(".*[ ,;{}()\n\t=].*")) {
          error(("JsonUtils.map2MapOp one_field exception: ").concat("key为：" + obj + "，值为：" + map.get(obj)))
        }
        newMap.put(TextUtils.filterFieldName(obj), map.getOrElse(obj, null))
      }
      //输出异常的全量日志
      if (matchesError) {
        error("JsonUtils.map2MapOp all_field exception: ".concat(map.toMap.mkString("\t")))
      }

      Some(newMap)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }

}
