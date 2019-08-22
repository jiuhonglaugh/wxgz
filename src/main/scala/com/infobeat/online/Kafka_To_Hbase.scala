package com.infobeat.online

import java.util.Properties

import com.infobeat.utils.Read_Filel
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Kafka_To_Hbase {
  private val ZOOKEEPER_HOST = "172.10.4.77:2181,172.10.4.78:2181,172.10.4.79:2181"
  private val KAFKA_BROKER = "172.10.4.78:9092,172.10.4.79:9092,172.10.4.78:9091,172.10.4.79:9091"
  private val TRANSACTION_GROUP = "test_wxgz"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
