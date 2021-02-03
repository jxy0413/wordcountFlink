package cn.bjfu.proj
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
//从web服务器的日志中，统计实时的访问流量
//统计每分钟的ip访问量，取出访问量最大的5个地址，每5秒更新一次
object ApacheLogAnalysis {
  case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)
  case class UrlViewCount(url:String,count:Long,windowEnd:Long)
  def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val prop = new Properties()
      prop.put("bootstrap.servers","Master:9092")
      prop.put("group.id","consumer-group")
      prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      prop.put("auto.offset.reset","latest")
      val stream = env.addSource(
          new FlinkKafkaConsumer011[String](
            "testApache",
            new SimpleStringSchema(),
             prop
          )
      )
      stream.map( line=>{
         val arr = line.split(" ")
         val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
         val timestamp = simpleDataFormat.parse(arr(3)).getTime
        ApacheLogEvent(
          arr(0),
          arr(2),
          timestamp,
          arr(5),
          arr(6)
        )})
          .assignAscendingTimestamps(_.eventTime)
          .keyBy(_.url)
          .timeWindow(Time.minutes(10),Time.seconds(5))
          .aggregate(new CountUrlAgg,new WindowUrlResultFunction)
          .keyBy(_.windowEnd)
          .process(new TopUrl(5)).print()
      env.execute()
  }
     class TopUrl(n: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
       lazy val urlState = getRuntimeContext.getListState(
         new ListStateDescriptor[UrlViewCount](
           "urlState-state",
            Types.of[UrlViewCount]
         )
       )
       override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
         //每条数据保存在状态里面
         urlState.add(value)
         ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
       }

       override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
         //获取所有URL访问量
         val allBuffered:ListBuffer[UrlViewCount] = ListBuffer()
         import scala.collection.JavaConversions._
         for(urlView <- urlState.get()){
             allBuffered+=urlView
         }
         //清除空间
         urlState.clear()
         //按照访问量大小排序
         val sortUrlViw = allBuffered.sortBy(-_.count).take(n)
         //将排名个格式化 便于打印
         var result = new StringBuilder
         result
           .append("========================\n")
           .append("时间：")
           .append(new Timestamp(timestamp-1))
           .append("\n")

         for(i<-sortUrlViw.indices){
             val currentView:UrlViewCount = sortUrlViw(i)
             result.append("NO")
             .append(i+1)
             .append(" URL=")
             .append(currentView.url)
             .append(" 流量=")
             .append(currentView.count)
             .append("\n")
         }
         result
           .append("========================\n\n\n")
         out.collect(result.toString())
       }
     }


     class CountUrlAgg extends AggregateFunction[ApacheLogEvent,Long,Long]{
       override def createAccumulator(): Long = 0L

       override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator+1

       override def getResult(accumulator: Long): Long = accumulator

       override def merge(a: Long, b: Long): Long = a+b
     }

     class WindowUrlResultFunction extends ProcessWindowFunction[Long,UrlViewCount,String,TimeWindow]{
       override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
         out.collect(UrlViewCount(key,elements.iterator.next(),context.window.getEnd))
       }
     }
}
