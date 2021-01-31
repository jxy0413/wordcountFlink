package cn.bjfu.day6

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object FlinkReadFromKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val prop = new Properties()
    prop.put("bootstrap.servers","Master:9092")
    prop.put("group.id","consumer-group")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("auto.offset.reset","latest")

    val stream = env
      .addSource(new FlinkKafkaConsumer011[String](
        "test01",
        new SimpleStringSchema(),
        prop
      ))

    stream.print()
    env.execute()
  }
}
