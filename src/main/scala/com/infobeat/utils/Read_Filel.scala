package com.infobeat.utils

import java.io.InputStream
import java.util.Properties

import org.slf4j.LoggerFactory

object Read_Filel {
  val LOGGER = LoggerFactory.getLogger(this.getClass)

  def getPro(filePath: String): Properties = {
    val pro = new Properties()

    pro.load(getInputStream(filePath))

    pro
  }

  def getInputStream(filePath: String): InputStream = {
    var inputStream: InputStream = null
    try {
      inputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream(filePath)
    } catch {
      case e: Exception =>
        LOGGER.error("获取文件流失败", e.printStackTrace())
    }
    inputStream
  }

}
