package cn.bjfu.day3

import cn.bjfu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AvgTempByAggAndProWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
    stream.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .aggregate(new AvgTempAgg,new WindowResult)
      .print()
    env.execute()
  }
}
//第一个泛型：流中元素的类型
//第二个泛型：累加器的泛型 元组（传感器ID,来了多少条温度，来的温度度数是多少）
//第三个反省：元组
class AvgTempAgg extends AggregateFunction[SensorReading,(String,Long,Double),(String,Double)]{
  override def createAccumulator(): (String, Long, Double) = ("",0L,0)
  //聚合逻辑是什么
  override def add(value: SensorReading, acc: (String, Long, Double)): (String, Long, Double) = {
    (value.id,acc._2+1,acc._3+value.temperature)
  }
  //窗口闭合时，输出的结果
  override def getResult(acc: (String, Long, Double)): (String, Double) = {
    (acc._1,acc._3/acc._2)
  }

  override def merge(acc: (String, Long, Double), acc1: (String, Long, Double)): (String, Long, Double) = {
    (acc._1,acc._2+acc1._2,acc._3+acc1._3)
  }
}
class WindowResult extends ProcessWindowFunction[(String,Double),AvgInfo,String,TimeWindow]{
  override def process(key: String, context: Context, elements: Iterable[(String, Double)], out: Collector[AvgInfo]): Unit = {
    //迭代器只有一个值，就是增量聚合函数的聚合结果
    out.collect(AvgInfo(key,elements.head._2,context.window.getStart,context.window.getEnd))
  }
}
case class AvgInfo(id:String,avgTemp:Double,windowStart:Long,windowEnd:Long)