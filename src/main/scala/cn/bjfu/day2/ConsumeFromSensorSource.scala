package cn.bjfu.day2

import org.apache.flink.streaming.api.scala._

object ConsumeFromSensorSource {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //调用addSource
    val stream = env.addSource(new SensorSource)
    stream.print()
    env.execute()
  }
}
