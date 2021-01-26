package cn.bjfu.day2


import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapExample1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements("jia_jia","shi_shi","sun_sun")

    stream.flatMap(new IdSplitrer).print()

    env.execute()
  }

  class MyFlatFunction extends FlatMapFunction[SensorReading,String]{
    override def flatMap(t: SensorReading, collector: Collector[String]): Unit = {
       collector.collect(t.id)
    }
  }

  class IdSplitrer extends FlatMapFunction[String,String]{
    override def flatMap(t: String, collector: Collector[String]): Unit = {
         val arr = t.split("_")
         arr.foreach(collector.collect)
    }
  }
}
