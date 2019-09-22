package com.infobeat.online

import java.net.URLDecoder
import java.util
import com.infobeat.common.customized.impl.DataEtlExec
import com.infobeat.common.customized.{BydDataETL, DefultDataETL}
import com.infobeat.utils.hbaseUtil.HbaseUtil
import com.infobeat.utils.redisUtil.RedisFieldData
import com.infobeat.utils.{Flink_Util, JSONUtil, Read_Filel}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.json.JSONObject
import org.slf4j.LoggerFactory

object Kafka_To_Hbase {

  private val LOGGER = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {


    val env = Flink_Util.createSeeWebUi
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val kafkaProps = Read_Filel.getPro("kafkaconsumer.properties")
    val topic = kafkaProps.getProperty("topic")
    kafkaProps.remove("topic")
    val dStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer010[String](topic,
        new SimpleStringSchema(),
        kafkaProps).setStartFromEarliest()
    )
    /**
     * 过滤无用数据
     * 根据前缀后缀 和 接口是否为 /publicLog/userInfo
     */
    val filterDS: DataStream[String] = dStream.map(decode => {
      var result = ""
      try {
        result = URLDecoder.decode(decode, "UTF-8")
      } catch {
        case ex: Exception => LOGGER.error(decode, ex.printStackTrace())
      }
      result
    }).filter(x => x.trim.startsWith("{") && x.trim.endsWith("}"))
      .filter(log => {
        val json = new JSONObject(log)
        !"/publicLog/userInfo".equals(json.getString("fieldUrl"))
      })
    /**
     * 判断厂商如有定制在这里判断
     * Byd  defult
     */
    val mfDs: DataStream[(String, JSONObject)] = filterDS.map(log => {
      val logJson: JSONObject = new JSONObject(log)
      val filedUrl: String = logJson.getString("fieldUrl")
      var fm = ""
      filedUrl match {
        case "/publicLog/bydApiYes" => fm = "Byd"
        case _ => fm = "defult"
      }
      (fm, logJson)
    })

    val result: DataStream[String] = mfDs.map(data => {
      val appkeyMap: util.Map[String, String] = RedisFieldData.getAppkeyMap
      var result: String = ""
      /**
       * 获取RowKey
       */
      val appkey = data._2.get("appKey")
      val diviced = data._2.get("deviceId").toString
      val appkey_key = new StringBuilder(appkeyMap.getOrDefault(appkey, ""))
        .append("|")
        .append(diviced)
        .toString()
      val new_rowKey = HbaseUtil.getRowKeyOfData(appkeyMap.get("appkey"), appkey_key)

      data._1 match {
        case "Byd" => result = DataEtlExec.setDataEtl(appkeyMap, data._2, new_rowKey, new BydDataETL)
        case "defult" => result = DataEtlExec.setDataEtl(appkeyMap, data._2, new_rowKey, new DefultDataETL)
        case _ => LOGGER.warn("没有这个厂商的数据请查看详细日志", data._2)
      }
      result
    }).filter(_.length > 1)

    val sink = new FlinkKafkaProducer010[String]("dianyou_wxgz", new SimpleStringSchema(), kafkaProps)
    //    result.print()
    result.addSink(sink)
    env.execute("Kafka_To_Hbase")
  }

  // 获取必要的属性名称
  def getNecessaryField: Set[String] = {
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

  def addNecessaryField(logJson: JSONObject, publicJson: JSONObject, necessaryFields: scala.collection.immutable.Set[String]): JSONObject = {
    val logMap = JSONUtil.json2Map(logJson)
    necessaryFields.foreach { x =>
      if (!logMap.keySet.contains(x) && publicJson.has(x)) {
        logJson.put(x, publicJson.getString(x))
      }
    }
    logJson
  }

}

