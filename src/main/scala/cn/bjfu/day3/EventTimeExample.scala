package cn.bjfu.day3

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EventTimeExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间为事件时间
    //系统默认每隔200ms插入一次水位线
    //下面的语句设置为每隔1分钟插入水位线
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("Master",9999)
      .map(line=>{
            val arr: Array[String] = line.split(" ")
            //时间时间的单位必须是毫秒
        (arr(0),arr(1).toLong*1000)
        //分配时间戳和水位线必须在keyBy之前进行
      }).assignTimestampsAndWatermarks(
         //设置最大的延迟时间是5s
         new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
           //告诉系统 时间戳是元组的第二个字段
           override def extractTimestamp(element: (String, Long)): Long = element._2
         }).keyBy(_._1)
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
}
