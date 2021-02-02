package cn.bjfu.day8

import cn.bjfu.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalaFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)

    val settings = EnvironmentSettings
        .newInstance()
        .useBlinkPlanner()
        .inStreamingMode()
        .build()

    val tEnv = StreamTableEnvironment.create(env,settings)

    val hashCode = new HashCode(10)
    tEnv.registerFunction("hashCode",hashCode)

    val table = tEnv.fromDataStream(stream)

//    table.select('id,hashCode('id))
//        .toAppendStream[Row].print()

    tEnv.createTemporaryView("sensor",table)

    tEnv.sqlQuery("select id,hashCode(id) from sensor").toAppendStream[Row].print()

    env.execute()
  }

  class HashCode(factor:Int) extends ScalarFunction{
      def eval(s:String):Int={
        s.hashCode * factor
      }
  }
}
