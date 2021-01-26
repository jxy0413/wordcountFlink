package cn.bjfu.day2

import org.apache.flink.streaming.api.scala._

object KeyStreamExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))
    //泛型变成了2个，第二个泛型是key的类型
    val keyed: KeyedStream[SensorReading, String] = stream.keyBy(_.id)
    //使用温度字段来做滚动聚合，求每个传感器的流上的最小值
    //内部会保存一个最小值的状态，用来保存到来的温度的最小值
    //滚动聚合后数据又变成了DataStream
    val value: DataStream[SensorReading] = keyed.min(2)
    //reduce也会保存用户的状态
    keyed.reduce((r1,r2)=>SensorReading(r1.id,0L,r1.temperature.min(r2.temperature))).print()
    env.execute()
  }
}
