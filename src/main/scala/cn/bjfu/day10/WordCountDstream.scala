package cn.bjfu.day10

import org.apache.flink.streaming.api.scala._
object WordCountDstream {
  def main(args: Array[String]): Unit = {
       val env = StreamExecutionEnvironment.getExecutionEnvironment
       val textDstream = env.socketTextStream("Master",9999)

       import org.apache.flink.api.scala._
       val treanDstream: DataStream[(String, Int)] = textDstream.flatMap(_.split(" "))
       .map((_, 1)).keyBy(0).sum(1)

       treanDstream.print()

       env.execute()

  }
}
