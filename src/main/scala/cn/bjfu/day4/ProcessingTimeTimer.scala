package cn.bjfu.day4

import java.sql.Timestamp

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessingTimeTimer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.socketTextStream("Master",9999)
      .map(line=>{
        var arr = line.split(" ")
        (arr(0),arr(1).toLong*1000)
      })
      .keyBy(_._1).process(new KeyedFun).print()
    env.execute()
  }
  class KeyedFun extends  KeyedProcessFunction[String,(String,Long),String]{
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      //注册一个定时器:时间携带的时间加上10S
     ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+2*1000L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("定时器触发了! 时间戳是："+new Timestamp(timestamp))
    }
  }
}
