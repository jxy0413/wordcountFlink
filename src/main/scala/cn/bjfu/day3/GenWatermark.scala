package cn.bjfu.day3

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object GenWatermark {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("Master",9999)
      .map(line=>{
        val arr: Array[String] = line.split(" ")
        //时间时间的单位必须是毫秒
        (arr(0),arr(1).toLong*1000)
        //分配时间戳和水位线必须在keyBy之前进行
      }).assignTimestampsAndWatermarks(
      //设置最大的延迟时间是5s
        new MyAssigner
      ).keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .process(new WindowResult)
    stream.print()
    env.execute()
  }
  class WindowResult extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(new Timestamp(context.window.getStart)+"~窗口中有"+elements.size+"个元素"+new Timestamp(context.window.getEnd))
    }
  }

  class MyAssigner extends AssignerWithPeriodicWatermarks[(String,Long)]{
    //设置最大延迟时间
    val bound:Long = 10*1000L
    //系统观察到系统的最大时间戳
    var maxTx:Long = Long.MinValue+bound
    override def getCurrentWatermark: Watermark = {
        new Watermark(maxTx-bound)
    }
    //每到一个事件就定义一次
    override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
      maxTx  = maxTx.max(element._2)
      element._2
    }
  }
}
