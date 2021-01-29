package cn.bjfu.day4

import cn.bjfu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoProcessFuncitonExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //第一条流，是一条无限流
    val readings = env.addSource(new SensorSource)
    //第二条流，是一个有限流
    //用来做开关，对'sensor_2'数据放行10s
    val switchs = env.fromElements(
      ("sensor_2",10*1000L)
    )
    val result = readings.connect(switchs)
      .keyBy(_.id,_._1) //on readings.id = switches._1
      .process(new ReadingFilter).print()
    env.execute()
  }

  class ReadingFilter extends CoProcessFunction[SensorReading,(String,Long),SensorReading]{
    //初始值为false
    lazy val forwardingEnable = getRuntimeContext.getState(
         new ValueStateDescriptor[Boolean]("switch",Types.of[Boolean])
    )
    //处理来自传感器的流数据
    override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      //如果开关是true，就认许数据向下流发送
      if(forwardingEnable.value()){
        out.collect(value)
      }
    }

    //处理来自开关流的数据
    override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      //打开开关
      forwardingEnable.update(true)
      val ts = ctx.timerService().currentProcessingTime()+value._2
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      //关闭开关
      forwardingEnable.clear()
    }
  }
}
