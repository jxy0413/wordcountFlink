package cn.bjfu.day2

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
    stream.map(new MyMapFunction).print()

    stream.map(new MapFunction[SensorReading,String] {
      override def map(t: SensorReading): String = t.id
    }).print()

    env.execute()
  }

  //输入SensorReading 输出String
  class MyMapFunction extends MapFunction[SensorReading,String]{
    override def map(t: SensorReading): String = t.id
  }
}
