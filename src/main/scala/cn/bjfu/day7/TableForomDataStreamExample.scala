package cn.bjfu.day7

import cn.bjfu.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TableForomDataStreamExample {
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
    //字段名必须以'开始，as用来起别名
    //DataStream => table
    val table:Table = tEnv.fromDataStream(stream)

    //table.toAppendStream[Row].print()

    //创建名字为sensor的临时表
    //使用数据流来创建临时表
    tEnv.createTemporaryView("sensor",stream)
    tEnv.sqlQuery("select *  from sensor where id = 'sensor_1'")
        .toAppendStream[Row] //追加流
        .print()
    env.execute()
  }
}
