package com.infobeat.utils

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * flink 工具类
 */
object Flink_Util {
  /**
   * 获取StreamExecutionEnvironment
   *
   * @return
   */
  def getSEE: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


  /**
   * 创建 StreamExecutionEnvironment
   *
   * @return
   */
  def createSEE: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()

  /**
   * 创建 StreamExecutionEnvironment
   * 指定并行度
   *
   * @param parallelism 并行度
   * @return
   */
  def creatSEE(parallelism: Int): StreamExecutionEnvironment = {
    StreamExecutionEnvironment.createLocalEnvironment(parallelism)
  }
}
