package com.infobeat.utils

import java.util.Properties

object Read_Filel {

  def getPro(fileName: String): Properties = {
    val pro = new Properties()
    pro.load(Thread.currentThread.getContextClassLoader.getResourceAsStream(fileName))
    pro
  }

}
