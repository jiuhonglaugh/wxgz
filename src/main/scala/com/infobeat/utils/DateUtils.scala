package com.infobeat.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

object DateUtils {
  /**
   *
   * @param urlTime
   * @return
   */
  def changeUrlTime2Date(urlTime: String): Date = {
    val formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
    val beginIndex = urlTime.indexOf("[")
    val endIndex = urlTime.lastIndexOf("]")
    val timeStr = urlTime.substring(beginIndex + 1, endIndex).split(" ")(0) + " +0800"
    formatter.parse(timeStr)
  }

  /**
   *
   * @param date
   * @return
   */
  def getSimpleDate(date: Date): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(date)
  }
}
