package com.infobeat.utils

import java.util.Properties

object Read_Filel {

  def getPro(filePath: String): Properties = {
    val pro = new Properties()
    pro.load(Thread.currentThread.getContextClassLoader.getResourceAsStream(filePath))
    pro
  }

}
