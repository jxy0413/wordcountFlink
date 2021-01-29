package cn.bjfu.day5

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowJoinExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //基于间隔的join只能使用事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream1 = env.fromElements(
         ("a",1,1000L),("b",2,2000L),("b",1,3000L))
      .assignAscendingTimestamps(_._3)

    val stream2 = env.fromElements(
        ("a",10,1000L),("b",2,2000L),("b",1,3000L))
      .assignAscendingTimestamps(_._3)

    stream1.join(stream2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new MyJoin)
      .print()
    env.execute()
  }
  //分流开窗后以后 属于同一个Input1和Input2的元素做笛卡尔
  //相同的key 且是相同的元素 做笛卡尔积
  class MyJoin extends JoinFunction[(String,Int,Long),(String,Int,Long),String]{
    override def join(first: (String, Int, Long), second: (String, Int, Long)): String = {
          first+"===>"+second
    }
  }
}
