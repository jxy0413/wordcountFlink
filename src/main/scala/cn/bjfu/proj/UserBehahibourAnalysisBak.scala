package cn.bjfu.proj

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object UserBehahibourAnalysisBak {
  case class UserBehavior(
                         userId:Long,
                         itemId:Long,
                         categoryId:Int,
                         behavior:String,
                         timestamp:Long
                         )
  case class ItemView(
                     itemId:Long,
                     windowEnd:Long,
                     count: Long
                     )
  def main(args: Array[String]): Unit = {
     val env = StreamExecutionEnvironment.getExecutionEnvironment
     env.setParallelism(1)
     //为了时间旅行，必须使用的事件时间
     env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
     val stream = env
        .readTextFile("C:\\Users\\Administrator\\IdeaProjects\\wordcountFlink\\src\\main\\resources\\UserBehavior.csv")
        .map(line => {
             val arr: Array[String] = line.split(",")
         UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong*1000L)
       })
         .filter(_.behavior.equals("pv"))
         .assignAscendingTimestamps(_.timestamp)
         .keyBy(_.itemId)//根据商品Id进行分类操作
         .timeWindow(Time.hours(1),Time.minutes(5))
         //增量聚合和全窗口使用
         .aggregate(new CountAggBak,new CountWindowBak)
         .keyBy(_.windowEnd)
         .process(new TopN(3))
         .print()
    env.execute()
  }

  class TopN(i: Int) extends KeyedProcessFunction[Long,ItemView,String]{
    lazy val itemState = getRuntimeContext.getListState(
       new ListStateDescriptor[ItemView]("itemCount",Types.of[ItemView])
    )
    //每来一条都会每次都会调用
    override def processElement(value: ItemView, ctx: KeyedProcessFunction[Long, ItemView, String]#Context, out: Collector[String]): Unit = {
          itemState.add(value)
          //由于所有value的windowEnd都一样,所以只会注册一个定时器
          ctx.timerService().registerEventTimeTimer(value.windowEnd+100L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemView, String]#OnTimerContext, out: Collector[String]): Unit = {
         val allItems:ListBuffer[ItemView] = ListBuffer()
         import scala.collection.JavaConversions._
         for(item <- itemState.get()){
              allItems+=item
         }
         itemState.clear()
         val sortedItems = allItems.sortBy(-_.count).take(i)
         //打印结果
         val result = new StringBuilder
         result.append("=====================").append("\n")
         .append("窗口结束时间是：")
         .append(new Timestamp(timestamp-100L))
         .append("\n")
         for(i<-sortedItems.indices){
             val curritm = sortedItems(i)
             result.append("第")
             .append(i+1)
             .append("名的商品ID是：")
             .append(curritm.itemId)
             .append("\t浏览量：")
             .append(curritm.count)
             .append("\n")
         }
          result.append("=====================")
          out.collect(result.toString())
    }
  }

  class CountAggBak extends AggregateFunction[UserBehavior,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehavior, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  class CountWindowBak extends ProcessWindowFunction[Long,ItemView,Long,TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemView]): Unit = {
        out.collect(ItemView(key,context.window.getEnd,elements.head))
    }
  }
}
