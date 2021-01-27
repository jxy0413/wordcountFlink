package cn.bjfu.day3

import java.lang

import cn.bjfu.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AvgTempByProcessWindowFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream =env.addSource(new SensorSource)
     stream
       .keyBy(_.id)
       .timeWindow(Time.seconds(5))
       .process(new AvgTempFunc)
       .print()
    env.execute()
  }
  //相比于增量聚合函数，缺点是保存所有元素
  //增量保留一个累加器就行了
  //优点是可以访问窗口信息的
  class AvgTempFunc extends  ProcessWindowFunction[SensorReading,AvgInfo,String,TimeWindow]{
    //在窗口闭合时调用
    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[AvgInfo]): Unit = {
          val count = elements.size //窗口闭合时，温度一共有多少条
          var sum = 0.0
          for(r<-elements){
            sum+=r.temperature
          }
          val windowStart = context.window.getStart
          val windowEnd = context.window.getEnd
          out.collect(AvgInfo(key,sum/count,windowStart,windowEnd))
    }
  }
}
