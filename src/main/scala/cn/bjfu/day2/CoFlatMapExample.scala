package cn.bjfu.day2

import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object CoFlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1:DataStream[(String,Int)] = env.fromElements(
      ("jiaxiangyu",175),
      ("shiqi",99)
    )
    val stream2:DataStream[(String,Int)] = env.fromElements(
      ("jiaxiangyu",25),
      ("shiqi",25)
    )
    val connectedStream: ConnectedStreams[(String, Int), (String, Int)] = stream1.keyBy(_._1).connect(stream2.keyBy(_._1))
    connectedStream.flatMap(new MyCoMapFunction).print()
    env.execute()
  }

  class MyCoMapFunction extends CoFlatMapFunction[(String, Int), (String, Int),String]{
    //map1处理来自第1条流的元素
    override def flatMap1(value: (String, Int), collector: Collector[String]): Unit = {
        collector.collect(value._1 + "的体重"+value._2+"斤")
        collector.collect(value._1 + "的体重"+value._2+"斤")
    }

    //map2处理来自第2条流的元素
    override def flatMap2(value: (String, Int), collector: Collector[String]): Unit = {
       collector.collect(value._1 + "的年龄"+value._2+"岁")
    }
  }

}
