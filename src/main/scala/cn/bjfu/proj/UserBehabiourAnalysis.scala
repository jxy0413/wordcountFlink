package cn.bjfu.proj

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object UserBehabiourAnalysis {
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
      .keyBy(_.itemId) //由于使用的是热门商品
      .timeWindow(Time.hours(1),Time.minutes(5)) //窗口聚合一小时 滑动5分钟
      //增量聚合和全窗口使用
      .aggregate(new CountAgg,new WindowResult)
      //对DataStream窗口结束时间进行分流
      //每一条支流的元素都属于每一个窗口
      //每一个支流按照count字段进行排序
      //支流的元素一定是有限的
      .keyBy(_.windowEnd) //=>keyedStream
      .process(new topN(3))
    stream.print()
    env.execute()
  }

  class topN(n:Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{
    //初始化一个列表状态变量
    lazy val itemState = getRuntimeContext.getListState(
       new ListStateDescriptor[ItemViewCount]("item-state",Types.of[ItemViewCount])
    )
    //每来一条ItemViewCount就调用一次
    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
        itemState.add(value)
        //由于所有value的windowEnd都一样,所以只会注册一个定时器
        ctx.timerService().registerEventTimeTimer(value.windowEnd+100L)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
        val allItems:ListBuffer[ItemViewCount] = ListBuffer()
        import scala.collection.JavaConversions._
        //因为列表没有排序功能，所以必须取出来
        for(item <- itemState.get()){
            allItems += item
        }
        itemState.clear()
        val sortedItems = allItems.sortBy(-_.count).take(n)
        //打印结果
        val result = new StringBuilder
        result.append("=====================").append("\n")
        .append("窗口结束时间是：")
          //还原窗口结束时间
        .append(new Timestamp(timestamp-100L))
        .append("\n")
       for(i<-sortedItems.indices){
          val curritm = sortedItems(i)
          result.append("第")
                .append(i+1)
           .append("名的商品ID是: ")
           .append(curritm.itemId)
           .append("浏览量：")
           .append(curritm.count)
           .append("\n")
       }
        result.append("=====================\n\n")
        out.collect(result.toString())
    }
  }

  class CountAgg extends AggregateFunction[UserBehaviour,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: UserBehaviour, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  //全窗口聚合函数的输入就是增量聚合函数的输出
  class WindowResult extends ProcessWindowFunction[Long,ItemViewCount,Long,TimeWindow]{
    override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.head))
    }
  }
}
