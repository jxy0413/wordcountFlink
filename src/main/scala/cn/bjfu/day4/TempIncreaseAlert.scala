package cn.bjfu.day4

import cn.bjfu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object TempIncreaseAlert {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
                    .keyBy(_.id)
      .process(new TempIncreateAlertFunction)
    stream.print
    env.execute()
  }
  class TempIncreateAlertFunction extends KeyedProcessFunction[String,SensorReading,String]{
    //初始化一个状态变量
    //懒加载
    //为什么不直接使用scala变量呢 var lastTemp:Double=_
    //通过配置，状态变量可以通过检查点操作，保存在hdfs上
    //当程序故障，可以从最近一次检查点恢复
    //所以要有一个名字和变量的类型（需要明确告诉flink状态变量的类型）
    lazy val lastTemp = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("last-temp",Types.of[Double])
    )
    //用来保存
    lazy val timerTs = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("ts",Types.of[Double])
    )
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      //如果来的是第一条温度，那么preTemp是0.0
      val preTemp = lastTemp.value()
      lastTemp.update(value.temperature)
      val curTimeTs = timerTs.value()
      if(preTemp==0.0||value.temperature<preTemp){
          //删除报警
         ctx.timerService().deleteProcessingTimeTimer(curTimeTs.toLong)
          //清空保存定时器时间戳
        timerTs.clear
      }else if(value.temperature>preTemp&&curTimeTs==0L){
         val ts = ctx.timerService().currentProcessingTime()+1000L
         ctx.timerService().registerProcessingTimeTimer(ts)
         timerTs.update(ts)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
       out.collect("传感器ID为"+ctx.getCurrentKey+"的传感器温度连续上升1S")
       timerTs.clear //清空定时器
    }
  }
}
