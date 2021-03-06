package cn.bjfu.day5

import cn.bjfu.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ListStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object ListStateExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.enableCheckpointing(1000L)
    env.setStateBackend(new FsStateBackend("file:\\D:\\1"))

    val stream = env
      .addSource(new SensorSource)
      .filter(_.id.equals("sensor_1"))
      .keyBy(_.id)
      .process(new KeyedF)
    stream.print()
    env.execute()
  }
  class KeyedF extends KeyedProcessFunction[String,SensorReading,String]{
    lazy  val listState = getRuntimeContext.getListState(
       new ListStateDescriptor[SensorReading]("list-state",Types.of[SensorReading])
    )
    lazy val timer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer",Types.of[Long])
    )
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      listState.add(value)
      if(timer.value()==0L){
         val ts = ctx.timerService().currentProcessingTime()+10*1000L
         ctx.timerService().registerProcessingTimeTimer(ts)
         timer.update(ts)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      //不能对列表状态进行计数
      val readings:ListBuffer[SensorReading ] = new ListBuffer[SensorReading]
      //隐士类型转换必须倒
      import scala.collection.JavaConversions._
      for(r <- listState.get()){
        readings+=r
      }
      out.collect("数据有"+readings.size)
      timer.clear()
    }
  }
}
