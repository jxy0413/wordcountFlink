package cn.bjfu.day4
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UpdateWindowResultWithLateElement {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("Master",9999)
      .map(line=>{
        var arr = line.split(" ")
        (arr(0),arr(1).toLong*1000L)
      })
      .assignTimestampsAndWatermarks(
        //最大延迟时间为5s
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
          override def extractTimestamp(element: (String, Long)): Long = element._2
        }
      )
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(5))
      //将迟到事件发送到测输出流中区
      .process(new UpdateWindowResult)
      stream.print()
      env.execute()
  }
    class UpdateWindowResult extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
      override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
        //当第一次对窗口进行求值时，也就是水位线超过窗口的结束事件的时候
        //会第一次调用process函数
        //这是isUpdate为默认值false
        //只对当前窗口可见
        val isUpdate = context.windowState.getState(
            new ValueStateDescriptor[Boolean]("update",Types.of[Boolean])
         )
        if(!isUpdate.value()){
          //当水位线超过窗口结束时，第一次调用
          out.collect("窗口第一次进行求值,元素共有"+elements.size+"个")
          isUpdate.update(true)
        }else{
          out.collect("迟到元素到来了！更新元素数量"+elements.size+"个")
        }
      }
    }
}

