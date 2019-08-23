package com.infobeat.online

import com.infobeat.utils.Read_Filel
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._

/**
 *
 */
object FlinkWouldCount {


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
    val result = dStream.flatMap(x => x.split(" "))
      .map(x => (x, 1)).keyBy(0).sum(1)

    result.print()

    env.execute("KafkaWordCount")
  }
}
