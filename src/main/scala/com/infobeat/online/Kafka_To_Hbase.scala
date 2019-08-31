package com.infobeat.online

import com.infobeat.utils.redisUtil.RedisFieldData
import com.infobeat.utils.{DataUtils, Flink_Util, JSONUtil, Read_Filel}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.json.JSONObject
import org.slf4j.LoggerFactory

object Kafka_To_Hbase {
  val LOGGER = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val env = Flink_Util.getSEE
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val kafkaProps = Read_Filel.getPro("kafkaconsumer.properties")
    val dStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer010[String](kafkaProps.getProperty("topic"),
        new SimpleStringSchema(),
        kafkaProps).setStartFromEarliest()
    )
    /**
     * 过滤无用数据
     * 根据前缀后缀 和 接口是否为 /publicLog/userInfo
     */
    val filterDS: DataStream[String] = dStream.filter(x => x.trim.startsWith("{") && x.trim.endsWith("}"))
      .filter(log => {
        val json = new JSONObject(log)
        !"/publicLog/userInfo".equals(json.getString("fieldUrl"))
      })
    /**
     * 判断厂商如有定制在这里判断
     * Byd
     */
    val mfDs: DataStream[(String, String)] = filterDS.map(log => {
      val json = new JSONObject(log)
      val filedUrl: String = json.getString("fieldUrl")
      var fm = ""
      filedUrl match {
        case "/publicLog/bydApiYes" => fm = "Byd"
        case _ => fm = "defult"
      }
      (fm, log)
    })
    mfDs.map(data => {
      val appkeyMap = RedisFieldData.appkeyMap

      val logJson = new JSONObject(data._2)
      if (!logJson.has("interfaceKey"))
        LOGGER.warn(s"日志中没有 interfaceKey ：$logJson")
      val time = DataUtils.changeUrlTime2Date(logJson.getString("urlTime"))
      val timeStr = DataUtils.getSimpleDate(time)
      logJson.put("time", timeStr)

      val logMap = JSONUtil.json2Map(logJson)
      val interFacekey = logMap.getOrElse("interfaceKey", "").toString
      val appkey = logMap.getOrElse("appKey", "").toString
      val necessaryFields = getNecessaryField() // 获取必有字段

      val logSet = logMap.keySet
      val set = necessaryFields -- logSet
 /*     if (set.nonEmpty) {
        // hbase中获取公有属性
        val rowKey = appkeyMap.getOrDefault(appkey, "") + "|" + logMap.getOrElse("deviceId", "").toString
        LOGGER.warn(s"Hbase查询的RowKey为：$rowKey")
        val publicJson = filterForPublicFields(rowKey, broadcast.value)
        logJson = addNecessaryField(logJson, publicJson, set)
      }*/

    })
    val sink = new FlinkKafkaProducer010[String]("sinkKafka", new SimpleStringSchema(), kafkaProps)
    mfDs.map(_._2).addSink(sink)
    env.execute("Kafka_To_Hbase")
  }

  // 获取必要的属性名称
  def getNecessaryField(): Set[String] = {
    var disSet = Set[String]()
    try {
      disSet = Read_Filel.getPro("application.properties")
        .getProperty("need_filed")
        .split(",")
        .toSet
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    disSet
  }

/*  def filterForPublicFields(rowKey: String, dir: String): JSONObject = {
    val date = new java.util.Date()
    val publicJson = new JSONObject()
    val tableName = Resources.getPro(dir, "hbase").getProperty("hbase.table.wxgz");
    val table = HbaseConnection.getTable(tableName, dir)
    try {
      val qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("_")) //列名不包含过滤器 不包含_

      val get = new Get(rowKey.getBytes())
      get.setFilter(qualifierFilter)
      val result = table.get(get)
      for (kv <- result.rawCells()) {
        publicJson.put(new String(kv.getQualifier), new String(kv.getValue))
      }
    } catch {
      case e: Exception => println(e)
    } finally {
      table.close()
    }
    val time = (new java.util.Date().getTime - date.getTime) / 1000
    LOGGER.warn(s"$rowKey 本次查询耗时：$time")
    publicJson
  }*/
}

