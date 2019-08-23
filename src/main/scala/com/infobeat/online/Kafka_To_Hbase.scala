package com.infobeat.online

import com.infobeat.utils.{Flink_Util, Read_Filel}
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Kafka_To_Hbase {
  def main(args: Array[String]): Unit = {
    val env = Flink_Util.getSEE
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val kafkaProps = Read_Filel.getPro("kafkaconsumer.properties")
    val dStream: DataStream[String] = env.addSource(
      new FlinkKafkaConsumer[String](kafkaProps.getProperty("topic"),
        new SimpleStringSchema(),
        kafkaProps).setStartFromEarliest()
    )
    dStream.filter(!_.trim.isEmpty)
      .map(line => {
        println(line)
      })
    env.execute("Kafka_To_Hbase")
  }
}
