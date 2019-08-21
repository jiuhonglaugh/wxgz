package com.infobeat

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._

/**
 * zhu
 */
object FlinkWouldCount {

  private val ZOOKEEPER_HOST = "172.10.4.77:2181,172.10.4.78:2181,172.10.4.79:2181"
  private val KAFKA_BROKER = "172.10.4.78:9092,172.10.4.79:9092,172.10.4.78:9091,172.10.4.79:9091"
  private val TRANSACTION_GROUP = "test_wxgz"

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
    val dStream = env.addSource(
      new FlinkKafkaConsumer[String]("dianyou_wxgz",
        new SimpleStringSchema(),
        kafkaProps).setStartFromEarliest()
    )
    val result = dStream.flatMap(x => x.split(" "))
      .map(x => (x, 1)).keyBy(0).sum(1)

    result.print()

    env.execute("KafkaWordCount")
  }
}
