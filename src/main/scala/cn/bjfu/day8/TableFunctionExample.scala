package cn.bjfu.day8

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions._
import org.apache.flink.types.Row

object TableFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
             "hello#world",
                    "atguigu#bigdata"
    )

    val settings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tEnv = StreamTableEnvironment.create(env,settings)
    //table写法
    val table: Table = tEnv.fromDataStream(stream,'s)
    val split = new Split("#")

//    table.
//      //为了将atguigu#bigdata,bigdata,7 join在一行
//      joinLateral(split('s)as('word,'length))
//      .select('s,'word,'length)
//      .toAppendStream[Row].print()

    tEnv.registerFunction("split",split)

    tEnv.createTemporaryView("t",table)

    tEnv
      //‘T的意思是元组,flink里面的固定语法
      .sqlQuery("select s,word,length from t,LateRal table (split(s)) as T(word,length)")
      // .sqlQuery("select s,word,length from t left join lateral table(split(s)) as T(word,length)")
        .toAppendStream[Row].print( )

    env.execute()
  }
  //输出的泛型是(String,Int)
  class Split(sep:String) extends TableFunction[(String,Int)]{
        def eval(s:String): Unit ={
             s.split(sep).foreach(x=>collect(x,x.length))
        }
  }

}
