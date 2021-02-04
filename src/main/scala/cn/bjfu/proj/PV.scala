package cn.bjfu.proj

import java.sql.Timestamp

import cn.bjfu.proj.UserBehabiourAnalysis.UserBehaviour
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object PV {
  case class UserBehaviour(
                            userId:Long,
                            itemId:Long,
                            categoryId:Int,
                            behaviour: String,
                            timeStamp:Long
                          )

  case class ItemViewCount(itemId:Long, //商品ID
                           windowEnd:Long, //窗口结束时间
                           count:Long

                          )
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //为了时间旅行 必须要用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env
      .readTextFile("C:\\Users\\Administrator\\IdeaProjects\\wordcountFlink\\src\\main\\resources\\UserBehavior.csv")
      .map(line=>{
        val arr: Array[String] = line.split(",")
        //注意，时间戳必须是毫秒
        UserBehaviour(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong*1000)
      })
      .filter(_.behaviour.equals("pv"))
      .assignAscendingTimestamps(_.timeStamp) //分配升序时间戳
      .timeWindowAll(Time.hours(1))
      .aggregate(new CountAggUv,new WindowResultUv)
      .print()
      env.execute()
  }
  class CountAggUv extends AggregateFunction[UserBehaviour,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehaviour, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }
  //ProcessWindowFunction 用于KeyBy
  //
  class WindowResultUv extends ProcessAllWindowFunction[Long,String,TimeWindow]{
    override def process(context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("窗口的结束时间为："+new Timestamp(context.window.getEnd)+"的窗口pv值是："+elements.head)
    }
  }
}
