package cn.bjfu.day7

import cn.bjfu.day2.SensorSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object EventTimeSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tEnv = StreamTableEnvironment.create(env,settings)

    val stream = env.addSource(new SensorSource).assignAscendingTimestamps(_.timestamp)

    //pt.proctime指定了处理时间是'pt;字段
    //指定已有字段为事件时间
    val table:Table = tEnv.fromDataStream(stream,'id,'timestamp.rowtime as 'ts,'temperature as 'temp,'pt.proctime)

    table.window(Tumble over 10.seconds on 'pt as 'w)
      .groupBy('id,'w)
      .select('id,'id.count)
      .toRetractStream[Row].print()


    env.execute()
  }
}
