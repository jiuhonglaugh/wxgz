package com.infobeat.utils

import java.util.regex.{Matcher, Pattern}

object TextUtils {
  //处理字段名字
  def toLowerCase(str: String): String = {
    var fileName = ""
    if (str != null) {
      fileName = str.toLowerCase
    }
    fileName
  }

  //处理字段名字
  def filterFieldName(str: String): String = {
    var fileName = ""
    if (str != null) {
      val p: Pattern = Pattern.compile("([.]|[*]|[\\[]|[\\,]|[;]|[\\{]|[\\}]|[(]|[=]|[)]|[\t]|[\n]|[ ]|[\\|]|[\\\\]|[?])*")
      val m: Matcher = p.matcher(str)
      fileName = m.replaceAll("")
    }
    fileName
  }

  def isExits(array: Array[String], element: String): Boolean = {
    var result: Boolean = false
    import scala.util.control.Breaks._
    breakable {
      if (array == null) {
        result = false
        break()
      }
      for (i <- 0 until array.length) {
        val str: String = array(i)
        if (element.equals(str)) {
          result = true
          break()
        }
      }
    }
    result
  }
}
