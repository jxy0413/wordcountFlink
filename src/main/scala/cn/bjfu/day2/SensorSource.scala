package cn.bjfu.day2

import java.util.Calendar
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import scala.util.Random
//泛型是'SensorReading'
class SensorSource extends RichParallelSourceFunction[SensorReading]{
  //表示数据源是否正常运行
  var running:Boolean = true
  //上下文参数用来发出数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random
    var curFtemp = (1 to 10).map{
       //使用高斯噪声产生随机温度值
       i=>("sensor_"+i,(rand.nextGaussian()*20))
    }
    //产生无限数据流
    while(running){
      curFtemp = curFtemp.map(
          t=>(t._1,t._2+(rand.nextGaussian()*0.5)))
          //产生ms为单位的时间戳
          val curTime = Calendar.getInstance.getTimeInMillis
          //发送
          curFtemp.foreach(t=>ctx.collect(SensorReading(t._1,curTime,t._2)))

          Thread.sleep(500)

    }
  }

  //当关闭Flink关闭
  override def cancel(): Unit =running= false
}
