package cn.bjfu.day7


import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row
object TableExample {
    def main(args: Array[String]): Unit = {
       val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
       env.setParallelism(1)
       val settings = EnvironmentSettings
         .newInstance()
         .useBlinkPlanner() //使用blink planner 是流批统一的
         .inStreamingMode()
         .build()
       val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

      tableEnv
        .connect(new FileSystem().path("C:\\Users\\Administrator\\IdeaProjects\\wordcountFlink\\src\\main\\resources\\sensor.txt"))
        .withFormat(new Csv())
        .withSchema(
            new Schema()
              .field("id",DataTypes.STRING())
              .field("timestamp",DataTypes.BIGINT())
              .field("temperature",DataTypes.DOUBLE())
        )
        .createTemporaryTable("sensorTable") //创建临时表

      val sensorTable = tableEnv.from("sensorTable")

      val result: Table = sensorTable.select("id,temperature")
        .filter("id = 'sensor_1'")
      tableEnv.toAppendStream[Row](result).print()

      val result1: Table = tableEnv.sqlQuery("select * from sensorTable where id = 'sensor_1'")

      tableEnv.toAppendStream[Row](result1).print()
      env.execute()
    }
}
