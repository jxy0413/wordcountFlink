package cn.bjfu.day7

import cn.bjfu.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object SQLProcTime {
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

    //pt.proctime指定了处理时间是'pt;字段
    val table:Table = tEnv.fromDataStream(stream,'id,'timestamp as 'ts,'temperature as 'temp,'pt.proctime)

    //开窗口，后命名'w
    table.window(Tumble over 10.seconds on 'pt as 'w)
        .groupBy('id,'w)
        .select('id,'id.count)
        .toRetractStream[Row].print()

    tEnv.createTemporaryView("sensor",table)

    //tEnv.sqlQuery("select id,count(id) from sensor ")
    env.execute()
  }
}
