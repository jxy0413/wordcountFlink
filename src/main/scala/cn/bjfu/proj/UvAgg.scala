package cn.bjfu.proj

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.Set


object UvAgg {
  case class UserBehaviour(
                            userId:Long,
                            itemId:Long,
                            categoryId:Int,
                            behaviour: String,
                            timeStamp:Long
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
      .map(r=>("key",r.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new WindowResultuvCount,new WindowResultUV)
      .print()
    env.execute()
  }

  //如果访问量很大怎么办？ 这里的方法会把pv数据放在窗口
  class WindowResultuvCount extends AggregateFunction[(String,Long),(Set[Long],Long),Long]{
    override def createAccumulator(): (Set[Long], Long) = (mutable.Set[Long](),0L)

    override def add(value: (String, Long), accumulator: (Set[Long], Long)): (Set[Long], Long) = {
      if(!accumulator._1.contains(value._2)){
             accumulator._1+=value._2
        (accumulator._1,accumulator._2+1)
      }else{
        accumulator
      }
    }

    override def getResult(accumulator: (Set[Long], Long)): Long = accumulator._2

    override def merge(a: (Set[Long], Long), b: (Set[Long], Long)): (Set[Long], Long) = ???
  }

  class WindowResultUV extends ProcessWindowFunction[Long,String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("窗口结束时间为 "+new Timestamp(context.window.getEnd)+"的窗口统计值为"+elements.head)
    }
  }
}
