package cn.bjfu.day6

import java.sql.{Connection, DriverManager, PreparedStatement}
import cn.bjfu.day2.{SensorReading, SensorSource}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object WriteToMysql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new SensorSource)
    stream.addSink(new MyJdbcSink)
    env.execute()
  }
  class MyJdbcSink extends RichSinkFunction[SensorReading]{
      var conn:Connection =  _
      var insertStmt : PreparedStatement = _
      var updateStmt : PreparedStatement = _

      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection(
           "jdbc:mysql://Master:3306/test",
            "root",
            "bjfu1022"
         )
         insertStmt  = conn.prepareStatement(
            "INSERT INTO temperatures(sensor,temp) values(?,?)"
         )
     }

    //执行sql
    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
         insertStmt.setString(1,value.id)
         insertStmt.setDouble(2,value.temperature)
         insertStmt.execute()
    }

    override def close(): Unit = {
        insertStmt.close()
        conn.close()
    }
  }
}
