package cn.bjfu.day4


import cn.bjfu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FreezingAlarm {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
       //没有keyBy 没有开窗
      .process(new FreezingAlarmFunction)
    //stream.print()
    stream.getSideOutput(new OutputTag[String]("freezing-alarm")).print()
    env.execute()
  }

  class FreezingAlarmFunction extends ProcessFunction[SensorReading,SensorReading]{
    lazy val  freezingAlarmout = new OutputTag[String]("freezing-alarm")
    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
        if(value.temperature<32.0){
          ctx.output(freezingAlarmout,s"${value.id}低温警报！")
        }
        out.collect(value)
    }
  }
}
