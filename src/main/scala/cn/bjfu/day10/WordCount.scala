package cn.bjfu.day10

import org.apache.flink.api.scala._


object WordCount {
  def main(args: Array[String]): Unit = {
     val env = ExecutionEnvironment.getExecutionEnvironment
     val inputDs = env.fromElements(
       "hello world"
     )
     val wordCountDs: AggregateDataSet[(String, Int)] = inputDs.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
     wordCountDs.print()

     env.execute()
  }
}
