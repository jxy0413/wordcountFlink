package cn.bjfu.day2

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.scala._

object FilterExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream.filter(r=>r.id.equals("sensor_1")).print()

    stream.filter(new FilterFunction[SensorReading] {
      override def filter(t: SensorReading): Boolean = t.id.equals("sensor_1")
    }).print()
    env.execute()
  }
}
