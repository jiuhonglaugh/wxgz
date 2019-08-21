package com.infobeat

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

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

    val source: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("dianyou_wxgz", new SimpleStringSchema(), kafkaProps)

    source.setStartFromEarliest()

    val dStream: DataStream[String] = env.addSource(source)
    import org.apache.flink.api.scala._
    val result = dStream.flatMap(x => x.split("\\s"))
      .map(x => (x, 1)).keyBy(0).timeWindow(Time.seconds(2l)).sum(1)

    result.setParallelism(1).print()

    env.execute("KafkaWordCount")
  }
}
