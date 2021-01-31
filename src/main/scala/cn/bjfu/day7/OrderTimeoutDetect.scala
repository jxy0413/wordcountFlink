package cn.bjfu.day7

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object OrderTimeoutDetect {
  case class OrderEvent(orderId:String,eventType:String,eventTime:Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.fromElements(
       OrderEvent("order_1","create",2000L),
       OrderEvent("order_2","create",3000L),
       OrderEvent("order_2","pay",4000L)
    ).assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    //定义的规则
    val pattern = Pattern
                .begin[OrderEvent]("create")
      .where(_.eventType.equals("create"))
      .next("pay")
      .where(_.eventType.equals("pay"))
      .within(Time.seconds(5))

    val patternStream = CEP.pattern(stream,pattern)

    //用来输出超时订单的信息
    //只有create事件，没有pay事件
    val orderTimeoutputTag = new OutputTag[String]("timeout")

    //超时的检测
    val timeoutFunc = (map:scala.collection.Map[String,Iterable[OrderEvent]],ts:Long,out:Collector[String])=>{
          println("在"+ts+"之前没有支付")
          val orderCreate = map("create").iterator.next()
          out.collect("超时订单的ID为 "+orderCreate.orderId)
    }
    //处理匿名函数用来支付成功的检测
    val selectFunc = (map:scala.collection.Map[String,Iterable[OrderEvent]],out:Collector[String])=>{
          val orderCreate = map("pay").iterator.next()
          out.collect("订单的ID为 "+orderCreate.orderId+"支付成功")
    }
    val detectStream = patternStream
      //flatSelect方法接受柯里化参数
      //第一个参数:检测出的超时信息发送到的测输出标签
      //第二个参数:用哪个函数处理超时信息
      //第三个参数:用来处理超时信息的函数
      .flatSelect(orderTimeoutputTag)(timeoutFunc)(selectFunc)
    //打印匹配成功的信息
    detectStream.print()
    //打印测输出的信息 超时信息
    detectStream.getSideOutput(orderTimeoutputTag).print()
    env.execute()
  }
}
