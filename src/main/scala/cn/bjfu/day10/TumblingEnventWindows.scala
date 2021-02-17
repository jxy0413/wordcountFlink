package cn.bjfu.day10

import java.sql.Timestamp

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TumblingEnventWindows {
  def main(args: Array[String]): Unit = {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val dataStream: DataStream[String] = env.socketTextStream("Master",9999).map { line =>
        val arr: Array[String] = line.split(" ")
        //时间单位必须是毫秒
        (arr(0), arr(1).toLong*1000)
      }.assignTimestampsAndWatermarks(
        //设置最大延迟时间为5s
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(2)) {
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      ).keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .process(new WindowResultDay10)

      dataStream.print()
      env.execute()
  }

  class WindowResultDay10 extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(new Timestamp(context.window.getStart)+" 窗口中共有 "+elements.size+"个元素"+new Timestamp(context.window.getEnd))
    }
  }

}
