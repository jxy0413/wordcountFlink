package cn.bjfu.day7

import cn.bjfu.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object SensorReadingCountBySQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env,settings)

    val stream = env.addSource(new SensorSource)
    tEnv.createTemporaryView("sensor",stream)

    tEnv.sqlQuery("select id,count(id) from sensor group by id")
      .toRetractStream[Row].print()

    env.execute()
  }
}
