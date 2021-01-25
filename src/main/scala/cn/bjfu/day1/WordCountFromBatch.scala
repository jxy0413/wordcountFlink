package cn.bjfu.day1

import org.apache.flink.streaming.api.scala._

object WordCountFromBatch {
  def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      //这句代码会像资源管理器申请一个任务插槽
      env.setParallelism(1)
      val stream = env.fromElements(
        "hello world",
         "hello scala"
      )
      import org.apache.flink.api.scala._
      val transform: DataStream[WordWithCount] = stream.flatMap { line =>
        line.split(" ")
      }
        .map(w => WordWithCount(w, 1))
        .keyBy(0)
        .sum(1)
       transform.print()
       env.execute()
  }
}
