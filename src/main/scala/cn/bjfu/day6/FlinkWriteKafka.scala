package cn.bjfu.day6

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object FlinkWriteKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      "hello","world"
    )

    stream.addSink(
       new FlinkKafkaProducer011[String](
         "Master:9092",
         "test01",
         new SimpleStringSchema()  //使用字符串格式写入kafka
       )
    )
    env.execute()
  }
}
