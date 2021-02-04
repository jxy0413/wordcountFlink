package cn.bjfu.proj

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.lang

import org.apache.flink.shaded.guava18.com.google.common.hash.{BloomFilter, Funnels}


object UVByBloomFiter {
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
      .aggregate(new WindowBloom,new WindowResultUV)
      .print()
    env.execute()
  }

  class WindowBloom extends AggregateFunction[(String,Long),(Long,BloomFilter[lang.Long]),Long]{
    override def createAccumulator(): (Long, BloomFilter[lang.Long]) ={
      //第一个参数，指定了布隆过滤器要过滤的类型是Long
      //第二个参数，指定了大概有多少不同的元素进行去重
      //第三个参数，误报率设置了1%
      (0, BloomFilter.create(Funnels.longFunnel(), 100000000, 0.01))
    }


    override def add(value: (String, Long), accumulator: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = {
        var bloom = accumulator._2
        var uvCount = accumulator._1
        //如果布隆过滤器没有碰到过value._2这个userid
        if(!bloom.mightContain(value._2)){
           bloom.put(value._2)
           uvCount += 1
        }
          (uvCount,bloom)
    }

    override def getResult(accumulator: (Long, BloomFilter[lang.Long])): Long = accumulator._1

    override def merge(a: (Long, BloomFilter[lang.Long]), b: (Long, BloomFilter[lang.Long])): (Long, BloomFilter[lang.Long]) = ???
  }


  class WindowResultUV extends ProcessWindowFunction[Long,String,String,TimeWindow]{
    override def process(key: String, context: Context, elements: Iterable[Long], out: Collector[String]): Unit = {
      out.collect("窗口结束时间为 "+new Timestamp(context.window.getEnd)+"的窗口统计值为"+elements.head)
    }
  }
}
