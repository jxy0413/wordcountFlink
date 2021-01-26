package cn.bjfu.day2

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object CoMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1:DataStream[(String,Int)] = env.fromElements(
      ("jiaxiangyu",175),
      ("shiqi",120)
    )
    val stream2:DataStream[(String,Int)] = env.fromElements(
      ("jiaxiangyu",25),
      ("shiqi",23)
    )
    val connectedStream: ConnectedStreams[(String, Int), (String, Int)] =
                           stream1.keyBy(_._1).connect(stream2.keyBy(_._1))
    connectedStream.map(new MyCoMapFunction).print()
    env.execute()
  }

  class MyCoMapFunction extends CoMapFunction[(String, Int), (String, Int),String]{
    //map1处理来自第1条流的元素
    override def map1(value: (String, Int)): String = {
         value._1 + "的体重"+value._2+"斤"
    }

    //map2处理来自第2条流的元素
    override def map2(value: (String, Int)): String = {
        value._1 + "的年龄"+value._2+"岁"
    }
  }
}
